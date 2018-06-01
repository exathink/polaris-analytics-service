# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from marshmallow import Schema, fields
from sqlalchemy import text
from polaris.common import db
from polaris.utils import datetime_utils


class ActivitySummary(Schema):
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    commit_count = fields.Integer(required=True)
    contributor_count = fields.Integer(required=True)

    def for_all_orgs(self):
        query = text(
            """
                SELECT
                  repo_summary.*,
                  contributor_aliases + contributor_keys AS contributor_count
                FROM
                  (
                    SELECT
                      count(DISTINCT organization_id) AS organizations,
                      min(earliest_commit)            AS earliest_commit,
                      max(latest_commit)              AS latest_commit,
                      sum(commit_count)               AS commit_count
                    FROM
                      repos.repositories
                  )
                    AS repo_summary
                  CROSS JOIN
                  (
                    SELECT count(id) AS contributor_aliases
                    FROM
                      repos.contributor_aliases
                    WHERE contributor_key IS NULL AND not robot
                  )
                    AS contributors_aliases
                  CROSS JOIN
                  (
                    SELECT count(DISTINCT contributor_key) AS contributor_keys
                    FROM
                      repos.contributor_aliases
                    WHERE contributor_key IS NOT NULL AND not robot
                  ) AS contributor_keys
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query).fetchall()
            return self.dumps(results, many=True)

    def for_account(self, account_key):
        query = text(
            """
                SELECT
                  repo_summary.*,
                  contributor_aliases + contributor_keys AS contributor_count
                FROM
                  (
                    SELECT
                      count(DISTINCT organizations.id) AS organizations,
                      min(earliest_commit)             AS earliest_commit,
                      max(latest_commit)               AS latest_commit,
                      sum(commit_count)                AS commit_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                    WHERE account_key = :account_key
                  )
                    AS repo_summary
                  CROSS JOIN
                  (
                    SELECT count(DISTINCT contributor_aliases.id) AS contributor_aliases
                    FROM
                      repos.contributor_aliases
                      INNER JOIN repos.repositories_contributor_aliases
                        ON contributor_aliases.id = repositories_contributor_aliases.contributor_alias_id
                      INNER JOIN repos.repositories ON repositories_contributor_aliases.repository_id = repositories.id
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                    WHERE account_key = :account_key AND contributor_key IS NULL AND not robot

                  )
                    AS contributors_aliases
                  CROSS JOIN
                  (
                    SELECT count(DISTINCT contributor_key) AS contributor_keys
                    FROM
                      repos.contributor_aliases
                      INNER JOIN repos.repositories_contributor_aliases
                        ON contributor_aliases.id = repositories_contributor_aliases.contributor_alias_id
                      INNER JOIN repos.repositories ON repositories_contributor_aliases.repository_id = repositories.id
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                    WHERE account_key = :account_key AND contributor_key IS NOT NULL AND not robot
                  ) AS contributor_keys
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(account_key=account_key)).fetchall()
            return self.dumps(results, many=True)

    def for_organization(self, organization_key):
        query = text(
            """
            SELECT
              earliest_commit,
              latest_commit,
              commit_count,
              contributor_count
            FROM
            (
              /* Aggregate commit stats for all repos in this organization*/
              SELECT
              organizations.id AS organization_id,
              sum(commit_count) AS commit_count,
              MIN (earliest_commit) AS earliest_commit,
              max(latest_commit) AS latest_commit
              FROM
              repos.organizations
              INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
              WHERE organizations.organization_key = :organization_key
              GROUP BY organizations.id
            ) 
            AS organization_repo_stats
            /* Now join this with the set of distinct contributors for the organization*/
            INNER JOIN
              (
                /* If a contributor_alias has a contributor key then the distinct contributor key is what 
                  counts as the contributor identity. Otherwise we use to the contributor_alias_id to denote
                  unique identity of a contributor. So the total distinct contributors in the org 
                  are the SUM of number of distinct contributor keys for aliases with contributor keys and the number of 
                  distinct contributor_aliases for the aliases without contributor keys
                */
                SELECT
                  min(organization_id)   AS org_id,
                  sum(contributor_count) AS contributor_count
                FROM
                  (
                    /* Number of distinct contributor aliases for aliases without keys*/
                    SELECT
                      organizations.id                     AS organization_id,
                      COUNT(DISTINCT contributor_alias_id) AS contributor_count
                    FROM
                      repos.organizations
                      INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                    WHERE organizations.organization_key = :organization_key AND contributor_key IS NULL AND not robot
                    GROUP BY organizations.id

                    UNION
                    /* Number of distinct contributor keys for aliases with contributor keys*/
                    SELECT
                      organizations.id                AS organization_id,
                      COUNT(DISTINCT contributor_key) AS contributor_count
                    FROM
                      repos.organizations
                      INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                    WHERE organizations.organization_key = :organization_key AND contributor_key IS NOT NULL AND not robot
                    GROUP BY organizations.id
                  ) 
                  AS _
                ) 
                AS organization_contributor_stats  
            ON organization_repo_stats.organization_id = organization_contributor_stats.org_id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_key=organization_key)).fetchall()
            return self.dumps(results, many=True)

    def for_project(self, project_key):

        query = text(
            """
                SELECT
                  earliest_commit,
                  latest_commit,
                  commit_count,
                  contributor_count
                FROM
                  (
                    SELECT
                      projects.id             AS project_id,
                      min(projects.name)      AS project,
                      min(earliest_commit)    AS earliest_commit,
                      max(latest_commit)      AS latest_commit,
                      sum(commit_count)       AS commit_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                    WHERE projects.project_key = :project_key
                    GROUP BY projects.id
                  )
                    AS repo_stats
                  INNER JOIN
                  (
                    SELECT
                      project_id,
                      sum(contributor_count) AS contributor_count
                    FROM
                      (
                        SELECT
                          projects.id                          AS project_id,
                          count(DISTINCT contributor_alias_id) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE projects.project_key = :project_key AND contributor_key IS NULL AND
                              NOT robot
                        GROUP BY projects.id
                        UNION
                        SELECT
                          projects.id                     AS project_id,
                          count(DISTINCT contributor_key) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE
                          projects.project_key = :project_key AND contributor_key IS NOT NULL AND
                          NOT robot
                        GROUP BY projects.id
                      ) AS _
                    GROUP BY project_id
                  ) AS repo_contributor_stats
                    ON repo_stats.project_id = repo_contributor_stats.project_id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(project_key=project_key)).fetchall()
            return self.dumps(results, many=True)

    def for_default_project(self, organization_name):
        query = text(
            """
                SELECT
                  earliest_commit,
                  latest_commit,
                  commit_count,
                  contributor_count
                FROM
                  (
                    SELECT
                      min(earliest_commit)    AS earliest_commit,
                      max(latest_commit)      AS latest_commit,
                      sum(commit_count)       AS commit_count
                    FROM
                      repos.repositories
                      LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                    WHERE organizations.name = :organization_name AND project_id IS NULL
                  )
                    AS repo_stats
                  CROSS JOIN
                  (
                    SELECT sum(contributor_count) AS contributor_count
                    FROM
                      (
                        SELECT count(DISTINCT contributor_alias_id) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                        WHERE organizations.name = :organization_name AND project_id IS NULL AND contributor_key IS NULL AND
                              NOT robot
                        UNION
                        SELECT count(DISTINCT contributor_key) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                        WHERE
                          organizations.name = :organization_name AND project_id IS NULL AND contributor_key IS NOT NULL AND
                          NOT robot
                      ) AS _
                  ) AS repo_contributor_stats

            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_name=organization_name)).fetchall()
            return self.dumps(results, many=True)




