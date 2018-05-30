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


class ActivityLevel(Schema):
    detail_instance_id = fields.Integer(required=True)
    detail_instance_name = fields.String(required=True)
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    commit_count = fields.Integer(required=True)
    contributor_count = fields.Integer(required=True)

    def for_all_orgs(self):
        query = text(
            """
              SELECT
                organizations.id AS detail_instance_id,
                organizations.name AS detail_instance_name,
                earliest_commit,
                latest_commit,
                commit_count,
                contributor_count
              FROM
                repos.organizations
              INNER JOIN
              (
                  SELECT
                    organization_id,
                    min(repositories.earliest_commit) AS earliest_commit,
                    max(repositories.latest_commit)   AS latest_commit,
                    sum(repositories.commit_count)    AS commit_count
                  FROM repos.repositories
                  GROUP BY organization_id
            ) AS org_repo_summary
                ON organizations.id = org_repo_summary.organization_id
            INNER JOIN (
                SELECT
                 organization_id,
                 sum(contributor_count) AS contributor_count
                FROM
                  (
                    SELECT
                      organization_id,
                      count(DISTINCT contributor_alias_id) AS contributor_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                    WHERE contributor_aliases.contributor_key IS NULL AND not robot
                    GROUP BY repositories.organization_id
                    UNION
                    SELECT
                      organization_id,
                      count(DISTINCT contributor_key) AS contributor_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                    WHERE contributor_aliases.contributor_key IS NOT NULL AND not robot
                    GROUP BY repositories.organization_id
                  ) _
                GROUP BY organization_id
            ) org_contributor_summary
            ON org_repo_summary.organization_id = org_contributor_summary.organization_id
          """
        )
        with db.create_session() as session:
            results = session.connection.execute(query).fetchall()
            return self.dumps(results, many=True)

    def for_account_by_organization(self, account_key):
        query = text(
            """
                SELECT
                      org_repo_summary.organization_id  AS detail_instance_id,
                      org_repo_summary.organization     AS detail_instance_name,
                      earliest_commit,
                      latest_commit,
                      commit_count,
                      contributor_count
                FROM
                (
                   SELECT
                     organizations.id                            AS organization_id,
                     min(organizations.organization_key :: TEXT) AS organization_key,
                     min(organizations.name)                     AS organization,
                     min(earliest_commit)                        AS earliest_commit,
                     max(latest_commit)                          AS latest_commit,
                     sum(commit_count)                           AS commit_count
                   FROM repos.repositories
                     INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                     INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                     INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                   WHERE account_key = :account_key
                   GROUP BY organizations.id
                ) AS org_repo_summary
                INNER JOIN
                (
                   SELECT
                     organization_id,
                     sum(contributor_count) AS contributor_count
                   FROM
                     (
                       SELECT
                         repositories.organization_id,
                         count(DISTINCT contributor_alias_id) AS contributor_count
                       FROM
                         repos.repositories
                         INNER JOIN repos.repositories_contributor_aliases
                           ON repositories.id = repositories_contributor_aliases.repository_id
                         INNER JOIN repos.contributor_aliases
                           ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                         INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                         INNER JOIN repos.accounts_organizations
                           ON accounts_organizations.organization_id = organizations.id
                         INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                       WHERE account_key = :account_key AND contributor_aliases.contributor_key IS NULL AND not robot
                       GROUP BY repositories.organization_id
                       UNION
                       SELECT
                         repositories.organization_id,
                         count(DISTINCT contributor_key) AS contributor_count
                       FROM
                         repos.repositories
                         INNER JOIN repos.repositories_contributor_aliases
                           ON repositories.id = repositories_contributor_aliases.repository_id
                         INNER JOIN repos.contributor_aliases
                           ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                         INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                         INNER JOIN repos.accounts_organizations
                           ON accounts_organizations.organization_id = organizations.id
                         INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                       WHERE account_key = :account_key AND contributor_aliases.contributor_key IS NOT NULL AND not robot
                       GROUP BY repositories.organization_id
                     ) _
                   GROUP BY organization_id
                ) AS org_contributor_summary
                ON org_repo_summary.organization_id = org_contributor_summary.organization_id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(account_key=account_key)).fetchall()
            return self.dumps(results, many=True)

    def for_organization_by_project(self, organization_name):
        query = text(
            """
              SELECT
                  coalesce(projects.id, -1) as detail_instance_id, 
                  coalesce(projects.name, 'Default') AS detail_instance_name,
                  project_summaries.earliest_commit     AS earliest_commit,
                  project_summaries.latest_commit       AS latest_commit,
                  project_summaries.commit_count        AS commit_count, 
                  project_summaries.contributor_count   AS contributor_count
            FROM
              (
                SELECT
                  prs.*, 
                  pcs.contributor_count
                FROM
                  (
                    SELECT
                      coalesce(project_id, -1) AS project_id,
                      min(projects.name)       AS project_name,
                      sum(commit_count)        AS commit_count,
                      min(earliest_commit)     AS earliest_commit,
                      max(latest_commit)       AS latest_commit
                    FROM
                      repos.organizations
                      INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                      LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      LEFT OUTER JOIN repos.projects ON projects_repositories.project_id = projects.id
                    WHERE organizations.name = :organization_name
                    GROUP BY project_id
                  ) AS prs
                  INNER JOIN
                  (
                    SELECT
                      coalesce(project_id, -1) AS project_id,
                      sum(contributor_count)   AS contributor_count
                    FROM
                      (
                        SELECT
                          project_id,
                          COUNT(DISTINCT contributor_alias_id) AS contributor_count
                        FROM
                          repos.organizations
                          INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          LEFT OUTER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE organizations.name = :organization_name AND contributor_aliases.contributor_key IS NULL AND not robot
                        GROUP BY project_id
                        UNION
                        SELECT
                          project_id,
                          COUNT(DISTINCT contributor_key) AS contributor_count
                        FROM
                          repos.organizations
                          INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          LEFT OUTER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE organizations.name = :organization_name AND contributor_aliases.contributor_key IS NOT NULL AND not robot
                        GROUP BY project_id
                      ) AS _
                    GROUP BY project_id
                  ) AS pcs ON prs.project_id = pcs.project_id
              ) AS project_summaries
            LEFT OUTER JOIN (
              SELECT 
                organizations.name as organization, 
                projects.*
              FROM 
                repos.projects
                INNER JOIN repos.organizations ON projects.organization_id = organizations.id
              WHERE organizations.name=:organization_name
            ) AS projects
            ON project_summaries.project_id=projects.id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_name=organization_name)).fetchall()
            return self.dumps(results, many=True)

    def for_project_by_repository(self, organization_name, project_name):
        if project_name == 'Default':
            return self.for_default_project_by_repository(organization_name)
        else:
            query = text(
                """
                    SELECT
                      repo_stats.repository_id as detail_instance_id,
                      repository as detail_instance_name,
                      earliest_commit,
                      latest_commit,
                      commit_count,
                      contributor_count
                    FROM
                      (
                        SELECT
                          repositories.id    AS repository_id,
                          repositories.name  AS repository,
                          earliest_commit,
                          latest_commit,
                          commit_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                          INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                        WHERE organizations.name = :organization_name AND projects.name = :project_name
                      )
                        AS repo_stats
                      INNER JOIN
                      (
                        SELECT
                          repository_id,
                          sum(contributor_count) AS contributor_count
                        FROM
                          (
                            SELECT
                              repositories.id                      AS repository_id,
                              count(DISTINCT contributor_alias_id) AS contributor_count
                            FROM
                              repos.repositories
                              INNER JOIN repos.repositories_contributor_aliases
                                ON repositories.id = repositories_contributor_aliases.repository_id
                              INNER JOIN repos.contributor_aliases
                                ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                              INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                              INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                              INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                            WHERE organizations.name = :organization_name AND projects.name = :project_name AND contributor_key IS NULL AND
                                  NOT robot
                            GROUP BY repositories.id
                            UNION
                            SELECT
                              repositories.id                 AS repository_id,
                              count(DISTINCT contributor_key) AS contributor_count
                            FROM
                              repos.repositories
                              INNER JOIN repos.repositories_contributor_aliases
                                ON repositories.id = repositories_contributor_aliases.repository_id
                              INNER JOIN repos.contributor_aliases
                                ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                              INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                              INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                              INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                            WHERE
                              organizations.name = :organization_name AND projects.name = :project_name AND contributor_key IS NOT NULL AND
                              NOT robot
                            GROUP BY repositories.id
                          ) AS _
                        GROUP BY repository_id
                      ) AS repo_contributor_stats
                        ON repo_stats.repository_id = repo_contributor_stats.repository_id
                """
            )
            with db.create_session() as session:
                results = session.connection.execute(query, dict(organization_name=organization_name, project_name=project_name)).fetchall()
                return self.dumps(results, many=True)

    def for_default_project_by_repository(self, organization_name):
        query = text(
            """
                SELECT
                  repo_stats.repository_id as detail_instance_id,
                  repository as detail_instance_name,
                  earliest_commit,
                  latest_commit,
                  commit_count,
                  contributor_count
                FROM
                  (
                    SELECT
                      repositories.id    AS repository_id,
                      organizations.name AS organization,
                      'Default'      AS project,
                      repositories.name  AS repository,
                      repositories.properties->>'project_group' AS repository_group,
                      repositories.properties->'tags'           AS tags,
                      repositories.properties->>'description'   AS description,
                      repositories.properties->>'website'       AS website,
                      repositories.properties->>'issues_url'    AS issues_url,
                      earliest_commit,
                      latest_commit,
                      commit_count
                    FROM
                      repos.repositories
                      LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                    WHERE organizations.name = :organization_name AND project_id is NULL
                  )
                    AS repo_stats
                  INNER JOIN
                  (
                    SELECT
                      repository_id,
                      sum(contributor_count) AS contributor_count
                    FROM
                      (
                        SELECT
                          repositories.id                      AS repository_id,
                          count(DISTINCT contributor_alias_id) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                        WHERE organizations.name = :organization_name AND project_id is NULL AND contributor_key IS NULL AND
                              NOT robot
                        GROUP BY repositories.id
                        UNION
                        SELECT
                          repositories.id                 AS repository_id,
                          count(DISTINCT contributor_key) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                        WHERE
                          organizations.name = :organization_name AND project_id is NULL AND contributor_key IS NOT NULL AND
                          NOT robot
                        GROUP BY repositories.id
                      ) AS _
                    GROUP BY repository_id
                  ) AS repo_contributor_stats
                    ON repo_stats.repository_id = repo_contributor_stats.repository_id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_name=organization_name)).fetchall()
            return self.dumps(results, many=True)