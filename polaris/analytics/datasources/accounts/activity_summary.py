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
                organizations.id as organization_id,
                organizations.organization_key,
                organizations.name as organization,
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
            LEFT OUTER JOIN (
                SELECT
                  organization_id,
                  count(DISTINCT contributor_alias_id) AS contributor_count
                FROM
                  repos.repositories
                  INNER JOIN repos.repositories_contributor_aliases
                  ON repositories.id = repositories_contributor_aliases.repository_id
                GROUP BY repositories.organization_id
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
                  org_repo_summary.organization_id as organization_id,
                  org_repo_summary.organization_key as organization_key,
                  org_repo_summary.organization as organization,
                  earliest_commit,
                  latest_commit,
                  commit_count,
                  contributor_count
                FROM (
                  SELECT
                    organizations.id as organization_id,
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
                ) as org_repo_summary
                INNER JOIN (
                  SELECT
                    organizations.id as organization_id,
                    count(distinct contributor_alias_id) as contributor_count
                  FROM
                    repos.repositories_contributor_aliases
                    INNER JOIN repos.repositories on repositories_contributor_aliases.repository_id = repositories.id
                    INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                    INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                    INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                  WHERE account_key = :account_key
                  GROUP BY organizations.id
                ) as org_contributor_summary
                on org_repo_summary.organization_id = org_contributor_summary.organization_id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(account_key=account_key)).fetchall()
            return self.dumps(results, many=True)



