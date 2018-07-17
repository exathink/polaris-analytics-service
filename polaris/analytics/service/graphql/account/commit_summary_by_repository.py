# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from sqlalchemy import text
from polaris.common import db

from ..interfaces import NamedNode, KeyIdResolverMixin, CommitSummary


class AccountCommitSummaryByRepository(graphene.ObjectType, KeyIdResolverMixin):
    class Meta:
        interfaces = (NamedNode, CommitSummary)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        query = """
          SELECT
            :type                                       AS type,
            repositories.key::text                      AS key,
            repositories.name                           AS name,
            repositories.earliest_commit                AS earliest_commit,
            repositories.latest_commit                  AS latest_commit,
            repositories.commit_count                   AS commit_count,
            repo_contributor_summary.contributor_count  AS contributor_count
          FROM
            repos.repositories
            INNER JOIN repos.organizations on repositories.organization_id = organizations.id
            INNER JOIN repos.accounts_organizations on organizations.id = accounts_organizations.organization_id
            INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
            INNER JOIN (
                SELECT
                 repository_id,
                 sum(contributor_count) AS contributor_count
                FROM
                  (
                    SELECT
                      repositories.id as repository_id,
                      count(DISTINCT contributor_alias_id) AS contributor_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                      INNER JOIN repos.organizations on repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations on organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
                    WHERE accounts.account_key = :account_key AND contributor_aliases.contributor_key IS NULL AND not robot
                    GROUP BY repositories.id
                    UNION
                    SELECT
                      repositories.id as repository_id,
                      count(DISTINCT contributor_alias_id) AS contributor_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                      INNER JOIN repos.organizations on repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations on organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
                    WHERE accounts.account_key = :account_key  AND contributor_aliases.contributor_key IS NOT NULL AND not robot
                    GROUP BY repositories.id
                  ) _
                  GROUP BY repository_id
              ) repo_contributor_summary
            ON repositories.id = repo_contributor_summary.repository_id
            WHERE accounts.account_key = :account_key
          """
        with db.create_session() as session:
            return session.connection.execute(text(query), dict(account_key=account.id,
                                                                type=kwargs.get('group_by').value)).fetchall()












