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


class AccountOrganizationsNodes:
    interface = NamedNode

    @classmethod
    def selectable(cls):
        return select([
            organizations.c.id,
            organizations.c.organization_key.label('key'),
            organizations.c.name
        ]).select_from(
            organizations.join(accounts_organizations).join(accounts)
        ).where(accounts.c.account_key == bindparam('account_key'))

class AccountOrganizationsCommitSummaries:
    interface = CommitSummary

    @staticmethod
    def selectable(account_organizations_nodes):
        return select([
            account_organizations_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            account_organizations_nodes.outerjoin(repositories, account_organizations_nodes.c.id == repositories.c.organization_id)
        ).group_by(account_organizations_nodes.c.id)


class AccountOrganizationsContributorSummaries:
    interface = ContributorSummary

    @staticmethod
    def selectable(account_organizations_nodes):

        contributor_summmary = select([
            account_organizations_nodes.c.id,
            contributor_aliases.c.id.label('ca_id'),
            contributor_aliases.c.contributor_key
        ]).select_from(
            account_organizations_nodes.outerjoin(
                repositories, repositories.c.organization_id == account_organizations_nodes.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            ).outerjoin(
                contributor_aliases, contributor_aliases.c.id == repositories_contributor_aliases.c.contributor_alias_id
            )
        ).where (
            contributor_aliases.c.robot == False
        ).cte('contributor_summary')

        unique_contributors = select([
            contributor_summmary.c.id,
            func.count(distinct(contributor_summmary.c.contributor_key)).label('unique_contributor_count'),
        ]).select_from(
            contributor_summmary
        ).group_by(contributor_summmary.c.id).cte('unique_contributors')

        unassigned_aliases = select([
            contributor_summmary.c.id,
            func.count(distinct(contributor_summmary.c.ca_id)).label('unassigned_alias_count')
        ]).select_from(
            contributor_summmary
        ).where(contributor_summmary.c.contributor_key == None).\
            group_by(contributor_summmary.c.id).cte('unassigned_aliases')

        return select([
            unique_contributors.c.id,
            case([
                (unassigned_aliases.c.unassigned_alias_count == None, 0),
            ], else_= unassigned_aliases.c.unassigned_alias_count).label('unassigned_alias_count'),
            unique_contributors.c.unique_contributor_count,
            (
                case([
                    (unassigned_aliases.c.unassigned_alias_count == None, 0),
                ], else_ = unassigned_aliases.c.unassigned_alias_count)
                + unique_contributors.c.unique_contributor_count
            ).label('contributor_count')
        ]).select_from(
            unique_contributors.outerjoin(unassigned_aliases, unique_contributors.c.id == unassigned_aliases.c.id)
        )


