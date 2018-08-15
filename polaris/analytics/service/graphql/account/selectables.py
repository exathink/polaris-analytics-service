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

from sqlalchemy import select, func, bindparam, and_, distinct

from polaris.graphql.interfaces import NamedNode
from polaris.repos.db.model import organizations, accounts_organizations, accounts, projects, repositories
from polaris.repos.db.schema import repositories, contributors, contributor_aliases, repositories_contributor_aliases
from ..interfaces import CommitSummary, ContributorCount


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
        ).where(accounts.c.account_key == bindparam('key'))


class AccountOrganizationsNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            organizations.c.id,
            organizations.c.organization_key.label('key'),
            organizations.c.name
        ]).select_from(
            organizations.join(
                accounts_organizations
            ).join(
                accounts
            )
        ).where(accounts.c.account_key == bindparam('key'))


class AccountProjectsNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            projects.c.id,
            projects.c.project_key.label('key'),
            projects.c.name
        ]).select_from(
            projects.join(
                organizations
            ).join(
                accounts_organizations
            ).join(
                accounts
            )
        ).where(accounts.c.account_key == bindparam('key'))


class AccountRepositoriesNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name
        ]).select_from(
            repositories.join(
                organizations
            ).join(
                accounts_organizations
            ).join(
                accounts
            )
        ).where(accounts.c.account_key == bindparam('key'))


class AccountContributorNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            contributors.c.id,
            contributors.c.key,
            contributors.c.name
        ]).select_from(
            contributors.join(
                contributor_aliases.join(
                    repositories_contributor_aliases
                ).join(
                    repositories.join(
                        organizations
                    ).join(
                        accounts_organizations
                    ).join(
                        accounts
                    )
                )
            )
        ).where(
            and_(
                accounts.c.account_key == bindparam('key'),
                contributor_aliases.c.robot == False
            )
        ).distinct()


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


class AccountContributorCount:
    interface = ContributorCount

    @staticmethod
    def selectable(account_node, **kwargs):
        return select([
            account_node.c.id,
            func.count(distinct(contributor_aliases.c.contributor_id)).label('contributor_count')
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
        ).group_by(account_node.c.id)
