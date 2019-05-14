# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from flask_security import current_user
import graphene

from polaris.graphql.exceptions import AccessDeniedException
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable
from .selectables import AccountNode, AccountCommitSummary, AccountContributorCount, AccountOrganizationsNodes, \
    AccountProjectsNodes, AccountRepositoriesNodes, AccountContributorNodes, AccountRecentlyActiveRepositoriesNodes,\
    AccountRecentlyActiveProjectsNodes, AccountRecentlyActiveOrganizationsNodes, AccountWorkItemsSourcesNodes

from ..contributor import ContributorsConnectionMixin
from ..interface_mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorCountResolverMixin
from ..interfaces import CommitSummary, ContributorCount
from ..organization import OrganizationsConnectionMixin, RecentlyActiveOrganizationsConnectionMixin
from ..project import ProjectsConnectionMixin, RecentlyActiveProjectsConnectionMixin
from ..repository import RepositoriesConnectionMixin, RecentlyActiveRepositoriesConnectionMixin
from ..work_items_source import WorkItemsSourcesConnectionMixin

class Account(
    # Interface Mixins
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    # ConnectionMixins
    OrganizationsConnectionMixin,
    ProjectsConnectionMixin,
    RepositoriesConnectionMixin,
    RecentlyActiveRepositoriesConnectionMixin,
    RecentlyActiveProjectsConnectionMixin,
    RecentlyActiveOrganizationsConnectionMixin,
    ContributorsConnectionMixin,
    WorkItemsSourcesConnectionMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorCount)
        named_node_resolver = AccountNode
        interface_resolvers = {
            'CommitSummary': AccountCommitSummary,
            'ContributorCount': AccountContributorCount
        }
        connection_node_resolvers = {
            'organizations': AccountOrganizationsNodes,
            'projects': AccountProjectsNodes,
            'repositories': AccountRepositoriesNodes,
            'work_items_sources': AccountWorkItemsSourcesNodes,
            'recently_active_repositories': AccountRecentlyActiveRepositoriesNodes,
            'recently_active_projects': AccountRecentlyActiveProjectsNodes,
            'recently_active_organizations': AccountRecentlyActiveOrganizationsNodes,
            'contributors': AccountContributorNodes
        }

    @classmethod
    def Field(cls, **kwargs):
        return super().Field(key_is_required=False, **kwargs)

    @classmethod
    def resolve_field(cls, info,  key=None, **kwargs):
        if key is None:
            key = str(current_user.account_key)

        if key == str(current_user.account_key):
            return cls.resolve_instance(key, **kwargs)
        else:
            raise AccessDeniedException('Access denied for specified account')
