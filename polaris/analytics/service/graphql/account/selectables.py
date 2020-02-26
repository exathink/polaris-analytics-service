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
from datetime import datetime, timedelta
from sqlalchemy import select, func, bindparam, and_, distinct, between, cast, Text, or_
from polaris.utils.datetime_utils import time_window
from polaris.graphql.interfaces import NamedNode, KeyIdNode
from polaris.analytics.db.model import organizations, accounts_organizations, accounts, \
    projects, repositories, projects_repositories, contributors, commits, \
    repositories_contributor_aliases, \
    work_items_sources, account_members, \
    feature_flags, feature_flag_enablements

from polaris.auth.db.model import users

from ..interfaces import CommitSummary, UserInfo, \
    ContributorCount, CommitCount, AccountInfo, \
    OwnerInfo, ScopedRole, FeatureFlagEnablementInfo

from polaris.graphql.base_classes import NamedNodeResolver, ConnectionResolver, InterfaceResolver


class AccountNode(NamedNodeResolver):
    interfaces = (NamedNode, OwnerInfo, AccountInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            accounts.c.id,
            accounts.c.key.label('key'),
            accounts.c.name,
            accounts.c.owner_key,
            accounts.c.created,
            accounts.c.updated
        ]).select_from(
            accounts
        ).where(accounts.c.key == bindparam('key'))


class AllAccountNodes(ConnectionResolver):
    interfaces = (NamedNode, OwnerInfo, AccountInfo)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            accounts.c.id,
            accounts.c.key,
            accounts.c.name,
            accounts.c.owner_key,
            accounts.c.created,
            accounts.c.updated
        ])

    @staticmethod
    def sort_order(all_accounts_nodes, **kwargs):
        return [all_accounts_nodes.c.created.desc().nullslast()]


class AccountOrganizationsNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            organizations.c.id,
            organizations.c.key.label('key'),
            organizations.c.name
        ]).select_from(
            organizations.join(
                accounts_organizations
            ).join(
                accounts
            )
        ).where(accounts.c.key == bindparam('key'))


class AccountRecentlyActiveOrganizationsNodes(ConnectionResolver):
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            organizations.c.id,
            func.min(cast(organizations.c.key, Text)).label('key'),
            func.min(organizations.c.name).label('name'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            accounts.join(
                accounts_organizations.join(
                    organizations.join(
                        repositories.join(
                            commits
                        )
                    )
                )
            )
        ).where(
            and_(
                accounts.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            organizations.c.id
        )

    @staticmethod
    def sort_order(account_recently_active_organizations, **kwargs):
        return [account_recently_active_organizations.c.commit_count.desc()]


class AccountFeatureFlagsNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            feature_flags.c.id,
            feature_flags.c.key,
            feature_flags.c.name,
            feature_flags.c.enable_all,
            feature_flags.c.active

        ]).where(
            and_(
                feature_flags.c.active,
                or_(
                    feature_flags.c.enable_all,
                    and_(
                        feature_flag_enablements.c.scope == kwargs.get('scope'),
                        feature_flag_enablements.c.scope_key == kwargs.get('scope_key')
                    )
                )
            )
        )


class AccountProjectsNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            projects.c.id,
            projects.c.key.label('key'),
            projects.c.name
        ]).select_from(
            projects.join(
                organizations
            ).join(
                accounts_organizations
            ).join(
                accounts
            )
        ).where(accounts.c.key == bindparam('key'))


class AccountWorkItemsSourcesNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(integration_type=None, **kwargs):
        query = select([
            work_items_sources.c.id,
            work_items_sources.c.key,
            work_items_sources.c.name
        ]).select_from(
            work_items_sources.join(
                organizations
            ).join(
                accounts_organizations
            ).join(
                accounts
            )
        ).where(accounts.c.key == bindparam('key'))
        if integration_type:
            query = query.where(work_items_sources.c.integration_type == integration_type)

        return query


class AccountRecentlyActiveProjectsNodes(ConnectionResolver):
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            projects.c.id,
            func.min(cast(projects.c.key, Text)).label('key'),
            func.min(projects.c.name).label('name'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            accounts.join(
                accounts_organizations.join(
                    organizations.join(
                        projects, organizations.c.id == projects.c.organization_id
                    ).join(
                        projects_repositories, projects_repositories.c.project_id == projects.c.id
                    ).join(
                        repositories, projects_repositories.c.repository_id == repositories.c.id
                    ).join(
                        commits
                    )
                )
            )
        ).where(
            and_(
                accounts.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            projects.c.id
        )

    @staticmethod
    def sort_order(account_recently_active_projects, **kwargs):
        return [account_recently_active_projects.c.commit_count.desc()]


class AccountRepositoriesNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
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
        ).where(accounts.c.key == bindparam('key'))


class AccountRecentlyActiveRepositoriesNodes(ConnectionResolver):
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            repositories.c.id,
            func.min(cast(repositories.c.key, Text)).label('key'),
            func.min(repositories.c.name).label('name'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            accounts.join(
                accounts_organizations.join(
                    organizations.join(
                        repositories.join(
                            commits
                        )
                    )
                )
            )
        ).where(
            and_(
                accounts.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            repositories.c.id
        )

    @staticmethod
    def sort_order(account_most_active_repository, **kwargs):
        return [account_most_active_repository.c.commit_count.desc()]


class AccountContributorNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            contributors.c.id,
            contributors.c.key,
            contributors.c.name,
            repositories_contributor_aliases.c.repository_id
        ]).select_from(
            contributors.join(
                # use denormalized relationship
                repositories_contributor_aliases, contributors.c.id == repositories_contributor_aliases.c.contributor_id
            ).join(
                repositories
            ).join(
                organizations
            ).join(
                accounts_organizations
            ).join(
                accounts
            )
        ).where(
            and_(
                accounts.c.key == bindparam('key'),
                repositories_contributor_aliases.c.robot == False
            )
        ).distinct()


class AccountUserNodes(ConnectionResolver):
    interfaces = (KeyIdNode, ScopedRole)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        # This looks different from other node resolvers since we
        # need to join users by key since it is in the auth schema.
        # the connection resolver mixin provides the join_field as the key
        # to ensure this happens.
        return select([
            account_members.c.user_key.label('key'),
            accounts.c.key.label('scope_key'),
            account_members.c.role
        ]).select_from(
            accounts.join(
                account_members
            )
        ).where(
            accounts.c.key == bindparam('key')
        )


class AccountCommitSummary(InterfaceResolver):
    interface = CommitSummary

    @staticmethod
    def interface_selector(account_nodes, **kwargs):
        return select([
            account_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            account_nodes.outerjoin(
                accounts_organizations, accounts_organizations.c.account_id == account_nodes.c.id
            ).outerjoin(
                organizations
            ).outerjoin(
                repositories
            )
        ).group_by(account_nodes.c.id)


class AccountContributorCount(InterfaceResolver):
    interface = ContributorCount

    @staticmethod
    def interface_selector(account_node, **kwargs):
        return select([
            account_node.c.id,
            func.count(distinct(repositories_contributor_aliases.c.contributor_id)).label('contributor_count')
        ]).select_from(
            account_node.outerjoin(
                accounts_organizations, accounts_organizations.c.account_id == account_node.c.id
            ).outerjoin(
                organizations
            ).outerjoin(
                repositories
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            )
        ).where(
            repositories_contributor_aliases.c.robot == False
        ).group_by(account_node.c.id)


class AccountUserInfo(InterfaceResolver):
    interface = UserInfo

    @staticmethod
    def interface_selector(account_node, **kwargs):
        return select([
            account_node.c.id,
            users.c.email,
            func.concat(users.c.first_name, ' ', users.c.last_name).label('name'),
            users.c.first_name,
            users.c.last_name

        ]).select_from(
            users
        ).where(
            users.c.key == account_node.c.owner_key
        )
