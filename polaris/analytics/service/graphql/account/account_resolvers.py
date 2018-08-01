# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, func, distinct, case, bindparam

from polaris.analytics.service.graphql.interfaces import NamedNode
from polaris.repos.db.model import organizations, accounts_organizations, accounts

from polaris.repos.db.schema import repositories, contributor_aliases, repositories_contributor_aliases

from ..interfaces import CommitSummary, ContributorSummary
from ..query_utils import select_contributor_summary

class AccountNode:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            accounts.c.id,
            accounts.c.account_key.label('key'),
            accounts.c.name
        ]).select_from(
            accounts
        ).where(accounts.c.account_key == bindparam('account_key'))

class AccountCommitSummary:
    interface = CommitSummary

    @staticmethod
    def selectable(account_node, **kwargs):
        return select([
            account_node.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            account_node.outerjoin(
                accounts_organizations, accounts_organizations.c.account_id == account_node.c.id
            ).outerjoin(
                organizations
            ).outerjoin(
                repositories
            )
        ).group_by(account_node.c.id)


class AccountContributorSummary:
    interface = ContributorSummary

    @staticmethod
    def selectable(account_node, **kwargs):

        contributor_nodes = select([
            account_node.c.id,
            contributor_aliases.c.id.label('ca_id'),
            contributor_aliases.c.contributor_key
        ]).select_from(
            account_node.outerjoin(
                accounts_organizations, accounts_organizations.c.account_id == account_node.c.id
            ).outerjoin(
                organizations
            ).outerjoin(
                repositories
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            ).outerjoin(
                contributor_aliases, contributor_aliases.c.id == repositories_contributor_aliases.c.contributor_alias_id
            )
        ).where (
            contributor_aliases.c.robot == False
        ).cte('contributor_nodes')


        return select_contributor_summary(contributor_nodes)






