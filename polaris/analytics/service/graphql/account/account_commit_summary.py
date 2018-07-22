# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text
from polaris.common import db

class AccountCommitSummary:

    @classmethod
    def resolve(cls, account_key, info, **kwargs):
        query = """
                SELECT
                  repo_summary.*,
                  contributor_aliases + contributor_keys AS contributor_count
                FROM
                  (
                    SELECT
                      min(account_key::text)                      AS key,
                      min(accounts.name)                          AS name, 
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
        with db.create_session() as session:
            return session.connection.execute(text(query), dict(account_key=account_key)).fetchone()
