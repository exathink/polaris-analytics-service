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
from polaris.repos.db.model import organizations


class ActivitySummaryByOrganization(Schema):
    organization_id = fields.Integer(required=True)
    organization_key = fields.String(required=True)
    organization = fields.String(required=True)
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    commit_count = fields.Integer(required=True)
    contributor_count = fields.Integer(required=True)

    def for_all_orgs(self):
        query = text(
            """
              SELECT
                organizations.id AS organization_id,
                organizations.organization_key,
                organizations.name AS organization,
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
                    WHERE contributor_aliases.contributor_key IS NULL
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
                    WHERE contributor_aliases.contributor_key IS NOT NULL
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

    def for_account(self, account_key):
        query = text(
            """
                SELECT
                      org_repo_summary.organization_id  AS organization_id,
                      org_repo_summary.organization_key AS organization_key,
                      org_repo_summary.organization     AS organization,
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
                       WHERE account_key = :account_key AND contributor_aliases.contributor_key IS NULL
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
                       WHERE account_key = :account_key AND contributor_aliases.contributor_key IS NOT NULL
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
                      min(latest_commit)              AS latest_commit,
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
                    WHERE contributor_key IS NULL
                  )
                    AS contributors_aliases
                  CROSS JOIN
                  (
                    SELECT count(DISTINCT contributor_key) AS contributor_keys
                    FROM
                      repos.contributor_aliases
                    WHERE contributor_key IS NOT NULL
                  ) AS contributor_keys
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query).fetchall()
            return self.dumps(results, many=True)

    def for_account(self, account_key):
        query = text (
            """
                SELECT
                  repo_summary.*,
                  contributor_aliases + contributor_keys AS contributor_count
                FROM
                  (
                    SELECT
                      count(DISTINCT organizations.id) AS organizations,
                      min(earliest_commit)             AS earliest_commit,
                      min(latest_commit)               AS latest_commit,
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
                    WHERE account_key = :account_key AND contributor_key IS NULL
                
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
                    WHERE account_key = :account_key AND contributor_key IS NOT NULL
                  ) AS contributor_keys
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(account_key=account_key)).fetchall()
            return self.dumps(results, many=True)
