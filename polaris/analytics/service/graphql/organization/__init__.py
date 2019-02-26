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
    OrganizationsContributorCount, OrganizationsWorkItemsSourceCount,  OrganizationProjectsNodes, \
    OrganizationRepositoriesNodes, OrganizationRecentlyActiveRepositoriesNodes, \
    OrganizationContributorNodes, OrganizationRecentlyActiveProjectsNodes, \
    OrganizationRecentlyActiveContributorNodes, OrganizationWeeklyContributorCount, \
    OrganizationCommitNodes, OrganizationWorkItemNodes, OrganizationWorkItemEventNodes


from ..interface_mixins import \
    KeyIdResolverMixin, \
    NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorCountResolverMixin, \
    ProjectCountResolverMixin, RepositoryCountResolverMixin, WorkItemsSourceCountResolverMixin

from ..interfaces import CommitSummary, ContributorCount, ProjectCount, RepositoryCount, WorkItemsSourceCount

from ..project import ProjectsConnectionMixin, RecentlyActiveProjectsConnectionMixin
from ..repository import RepositoriesConnectionMixin, RecentlyActiveRepositoriesConnectionMixin
from ..contributor import ContributorsConnectionMixin, RecentlyActiveContributorsConnectionMixin

from ..selectable_field_mixins import WeeklyContributorCountsResolverMixin
from ..commit import CommitsConnectionMixin
from ..work_item import WorkItemsConnectionMixin, WorkItemEventsConnectionMixin


class Organization(
    # Interface Mixins
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    ProjectCountResolverMixin,
    RepositoryCountResolverMixin,
    WorkItemsSourceCountResolverMixin,
    # Connection Mixins
    ProjectsConnectionMixin,
    ContributorsConnectionMixin,
    RepositoriesConnectionMixin,
    RecentlyActiveRepositoriesConnectionMixin,
    RecentlyActiveContributorsConnectionMixin,
    RecentlyActiveProjectsConnectionMixin,
    CommitsConnectionMixin,
    WorkItemsConnectionMixin,
    WorkItemEventsConnectionMixin,
    # field mixins
    WeeklyContributorCountsResolverMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorCount, ProjectCount, RepositoryCount, WorkItemsSourceCount)
        named_node_resolver = OrganizationNode
        interface_resolvers = {
            'CommitSummary': OrganizationsCommitSummary,
            'ContributorCount': OrganizationsContributorCount,
            'ProjectCount': OrganizationsProjectCount,
            'RepositoryCount': OrganizationsRepositoryCount,
            'WorkItemsSourceCount': OrganizationsWorkItemsSourceCount,
        }
        connection_node_resolvers = {
            'projects': OrganizationProjectsNodes,
            'contributors': OrganizationContributorNodes,
            'commits': OrganizationCommitNodes,
            'repositories': OrganizationRepositoriesNodes,
            'work_items': OrganizationWorkItemNodes,
            'work_item_events': OrganizationWorkItemEventNodes,
            'recently_active_projects': OrganizationRecentlyActiveProjectsNodes,
            'recently_active_repositories': OrganizationRecentlyActiveRepositoriesNodes,
            'recently_active_contributors': OrganizationRecentlyActiveContributorNodes
        }
        selectable_field_resolvers = {
            'weekly_contributor_counts': OrganizationWeeklyContributorCount
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

    recently_active_organizations = Organization.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime,
            required=False,
            description="End date of period to search for activity. If not specified it defaults to utc now"
        ),
        days=graphene.Argument(
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
