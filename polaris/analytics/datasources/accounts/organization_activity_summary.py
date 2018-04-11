# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from marshmallow import Schema, fields
from sqlalchemy import text, select
from polaris.common import db
from polaris.utils import datetime_utils
from polaris.repos.db.model import organizations


class OrganizationActivitySummary(Schema):
    organization_key = fields.String(required=True)
    organization = fields.String(required=True)
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    commit_count = fields.Integer(required=True)
    contributor_count = fields.Integer(required=True)

    def for_all_orgs(self):
        query = text(self.all_orgs_summary())
        with db.create_session() as session:
            results = session.connection.execute(query).fetchall()
            return self.dumps(results, many=True)

    def for_account(self, account_key):
        query = text(
            self.all_orgs_summary() +
            """
                WHERE organizations.organization_key 
                IN (
                  SELECT organization_key from
                  auth.accounts
                  INNER JOIN auth.accounts_organizations on accounts.id = accounts_organizations.account_id
                  WHERE accounts.account_key = :account_key
                )
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(account_key=account_key)).fetchall()
            return self.dumps(results, many=True)


    def all_orgs_summary(self):
        return """
              SELECT
                organizations.organization_key,
                organizations.name,
                earliest_commit,
                latest_commit,
                commit_count,
                contributor_count
              FROM
                repos.organizations
              INNER JOIN
              (
                  SELECT
                    organization_key,
                    min(repositories.earliest_commit) AS earliest_commit,
                    max(repositories.latest_commit)   AS latest_commit,
                    sum(repositories.commit_count)    AS commit_count
                  FROM repos.repositories
                  GROUP BY organization_key
            ) AS org_repo_summary
                ON organizations.organization_key = org_repo_summary.organization_key
            LEFT OUTER JOIN (
                SELECT
                  organization_key,
                  count(DISTINCT contributor_alias_id) AS contributor_count
                FROM
                  repos.repositories
                  INNER JOIN repos.repositories_contributor_aliases
                  ON repositories.id = repositories_contributor_aliases.repository_id
                GROUP BY repositories.organization_key
            ) org_contributor_summary
            ON org_repo_summary.organization_key = org_contributor_summary.organization_key
          """
