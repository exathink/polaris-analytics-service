# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.analytics.service.graphql.summaries import ActivityLevelSummary, InceptionsSummary
from polaris.analytics.service.graphql.summary_mixins import ActivityLevelSummaryResolverMixin, InceptionsResolverMixin
from polaris.graphql.connection_utils import CountableConnection
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from .selectables import \
    OrganizationNode, OrganizationsCommitSummary, OrganizationsProjectCount, OrganizationsRepositoryCount, \
    OrganizationsContributorCount, OrganizationProjectsNodes, \
    OrganizationRepositoriesNodes, OrganizationRecentlyActiveRepositoriesNodes, \
    OrganizationContributorNodes, OrganizationRecentlyActiveProjectsNodes, \
    OrganizationRecentlyActiveContributorNodes


from ..interface_mixins import \
    KeyIdResolverMixin, \
    NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorCountResolverMixin, \
    ProjectCountResolverMixin, RepositoryCountResolverMixin

from ..interfaces import CommitSummary, ContributorCount, ProjectCount, RepositoryCount

from ..project import ProjectsConnectionMixin, RecentlyActiveProjectsConnectionMixin
from ..repository import RepositoriesConnectionMixin, RecentlyActiveRepositoriesConnectionMixin
from ..contributor import ContributorsConnectionMixin, RecentlyActiveContributorsConnectionMixin


class Organization(
    # Interface Mixins
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    ProjectCountResolverMixin,
    RepositoryCountResolverMixin,
    # Connection Mixins
    ProjectsConnectionMixin,
    ContributorsConnectionMixin,
    RepositoriesConnectionMixin,
    RecentlyActiveRepositoriesConnectionMixin,
    RecentlyActiveContributorsConnectionMixin,
    RecentlyActiveProjectsConnectionMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorCount, ProjectCount, RepositoryCount)
        named_node_resolver = OrganizationNode
        interface_resolvers = {
            'CommitSummary': OrganizationsCommitSummary,
            'ContributorCount': OrganizationsContributorCount,
            'ProjectCount': OrganizationsProjectCount,
            'RepositoryCount': OrganizationsRepositoryCount
        }
        connection_node_resolvers = {
            'projects': OrganizationProjectsNodes,
            'contributors': OrganizationContributorNodes,
            'repositories': OrganizationRepositoriesNodes,
            'recently_active_projects': OrganizationRecentlyActiveProjectsNodes,
            'recently_active_repositories': OrganizationRecentlyActiveRepositoriesNodes,
            'recently_active_contributors': OrganizationRecentlyActiveContributorNodes
        }

        connection_class = lambda: Organizations


    @classmethod
    def resolve_field(cls, parent, info, organization_key, **kwargs):
        return cls.resolve_instance(key=organization_key, **kwargs)



class Organizations(
    ActivityLevelSummaryResolverMixin,
    InceptionsResolverMixin,
    CountableConnection
):
    class Meta:
        node = Organization
        summaries = (ActivityLevelSummary, graphene.List(InceptionsSummary))


class OrganizationsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):

    organizations = Organization.ConnectionField()

    def resolve_organizations(self, info, **kwargs):
        return Organization.resolve_connection(
            self.get_connection_resolver_context('organizations'),
            self.get_connection_node_resolver('organizations'),
            self.get_instance_query_params(),
            **kwargs
        )


class RecentlyActiveOrganizationsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):

    recently_active_organizations = Organization.ConnectionField(days=graphene.Argument(
            graphene.Int,
            required=False,
            default_value=7,
            description="Return organizations with commits within the specified number of days"
        ))

    def resolve_recently_active_organizations(self, info, **kwargs):
        return Organization.resolve_connection(
            self.get_connection_resolver_context('recently_active_organizations'),
            self.get_connection_node_resolver('recently_active_organizations'),
            self.get_instance_query_params(),
            **kwargs
        )
