# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from sqlalchemy import text
from polaris.common import db

from ..interfaces import NamedNode, CommitSummary
from polaris.analytics.service.graphql.mixins import KeyIdResolverMixin


class AccountCommitSummaryByOrganization(graphene.ObjectType, KeyIdResolverMixin):
    class Meta:
        interfaces = (NamedNode, CommitSummary)

    @classmethod
    def resolve(cls, account_key, info, **kwargs):
        query = """
                SELECT
                      org_repo_summary.organization_key::text  AS key,
                      org_repo_summary.organization     AS name,
                      earliest_commit,
                      latest_commit,
                      commit_count,
                      contributor_count
                FROM
                (
                   SELECT
                     organizations.id                            AS organization_id,
                     min(organizations.organization_key::text)   AS organization_key,
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
        with db.create_session() as session:
            return session.connection.execute(text(query), dict(account_key=account_key)).fetchall()




