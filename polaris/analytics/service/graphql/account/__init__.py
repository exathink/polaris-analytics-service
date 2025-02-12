# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from flask_security import current_user
import graphene
import os
from polaris.graphql.exceptions import AccessDeniedException
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable, CountableConnection, ConnectionResolverMixin
from .selectables import AccountNode, AccountCommitSummary, AccountUserInfo, AccountContributorCount, \
    AccountOrganizationsNodes, \
    AccountProjectsNodes, AccountRepositoriesNodes, AccountContributorNodes, AccountRecentlyActiveRepositoriesNodes, \
    AccountRecentlyActiveProjectsNodes, AccountRecentlyActiveOrganizationsNodes, AccountWorkItemsSourcesNodes, \
    AllAccountNodes, AccountUserNodes
from polaris.analytics.service.graphql.feature_flag.selectable import ScopedFeatureFlagsNodes

from ..contributor import ContributorsConnectionMixin
from ..interface_mixins import NamedNodeResolverMixin, ContributorAliasesInfoResolverMixin
from ..interfaces import AccountInfo, CommitSummary, ContributorCount, UserInfo, OwnerInfo
from ..organization import OrganizationsConnectionMixin, RecentlyActiveOrganizationsConnectionMixin
from ..project import ProjectsConnectionMixin, RecentlyActiveProjectsConnectionMixin
from ..repository import RepositoriesConnectionMixin, RecentlyActiveRepositoriesConnectionMixin
from ..work_items_source import WorkItemsSourcesConnectionMixin
from ..user import UsersConnectionMixin
from ..feature_flag import FeatureFlagsConnectionMixin


class Account(
    # Interface Mixins
    NamedNodeResolverMixin,
    ContributorAliasesInfoResolverMixin,
    # ConnectionMixins
    OrganizationsConnectionMixin,
    ProjectsConnectionMixin,
    RepositoriesConnectionMixin,
    RecentlyActiveRepositoriesConnectionMixin,
    RecentlyActiveProjectsConnectionMixin,
    RecentlyActiveOrganizationsConnectionMixin,
    ContributorsConnectionMixin,
    WorkItemsSourcesConnectionMixin,
    UsersConnectionMixin,
    FeatureFlagsConnectionMixin,

    #
    Selectable
):
    def resolve_feature_flags(self, info, **kwargs):
        return super().resolve_feature_flags(info, scope='account', scope_key=self.key, **kwargs)

    class Meta:
        interfaces = (NamedNode, OwnerInfo, UserInfo, AccountInfo, CommitSummary, ContributorCount)
        named_node_resolver = AccountNode
        connection_class = lambda: Accounts

        interface_resolvers = {
            'CommitSummary': AccountCommitSummary,
            'ContributorCount': AccountContributorCount,
            'UserInfo': AccountUserInfo
        }
        connection_node_resolvers = {
            'organizations': AccountOrganizationsNodes,
            'projects': AccountProjectsNodes,
            'repositories': AccountRepositoriesNodes,
            'work_items_sources': AccountWorkItemsSourcesNodes,
            'recently_active_repositories': AccountRecentlyActiveRepositoriesNodes,
            'recently_active_projects': AccountRecentlyActiveProjectsNodes,
            'recently_active_organizations': AccountRecentlyActiveOrganizationsNodes,
            'contributors': AccountContributorNodes,
            'users': AccountUserNodes,
            'feature_flags': ScopedFeatureFlagsNodes,
        }

    @classmethod
    def Field(cls, key_is_required=False, **kwargs):
        return super().Field(
            key_is_required,
            contributor_count_days=graphene.Argument(
                graphene.Int,
                required=False,
                description="When evaluating contributor count "
                            "return only contributors that have committed code to the project in this many days"
            )
        )

    @classmethod
    def check_access(cls, key):
        return key == str(current_user.account_key) or 'admin' in current_user.role_names

    @classmethod
    def resolve_field(cls, info, key=None, **kwargs):
        if key is None:
            key = str(current_user.account_key)

        if cls.check_access(key):
            return cls.resolve_instance(key, **kwargs)
        else:
            raise AccessDeniedException('Access denied for specified account')

    @classmethod
    def resolve_all_accounts(cls, info, **kwargs):
        if 'admin' in current_user.role_names:
            return cls.resolve_connection(
                'all_accounts',
                AllAccountNodes,
                params=None,
                **kwargs
            )
        else:
            raise AccessDeniedException('Access denied')


class Accounts(
    CountableConnection
):
    class Meta:
        node = Account


class AccountsConnectionMixin(ConnectionResolverMixin):
    accounts = Account.ConnectionField()

    def resolve_accounts(self, info, **kwargs):
        return Account.resolve_connection(
            self.get_connection_resolver_context('accounts'),
            self.get_connection_node_resolver('accounts'),
            self.get_instance_query_params(),
            **kwargs
        )
