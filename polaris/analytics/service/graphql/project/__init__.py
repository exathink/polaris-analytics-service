# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable, ConnectionResolverMixin

from ..interfaces import CommitSummary, ContributorCount, RepositoryCount, OrganizationRef
from ..interface_mixins import \
    KeyIdResolverMixin, \
    NamedNodeResolverMixin, \
    CommitSummaryResolverMixin, \
    ContributorCountResolverMixin, \
    RepositoryCountResolverMixin, \
    OrganizationRefResolverMixin


from ..summaries import ActivityLevelSummary, InceptionsSummary
from ..summary_mixins import \
    ActivityLevelSummaryResolverMixin, \
    InceptionsResolverMixin

from ..selectable_field_mixins import CumulativeCommitCountResolverMixin


from ..repository import RepositoriesConnectionMixin, RecentlyActiveRepositoriesConnectionMixin
from ..contributor import ContributorsConnectionMixin

from .selectables import ProjectNode, \
    ProjectRepositoriesNodes, \
    ProjectContributorNodes, \
    ProjectsContributorCount, \
    ProjectsCommitSummary, \
    ProjectsRepositoryCount, \
    ProjectsOrganizationRef, \
    ProjectRecentlyActiveRepositoriesNodes, \
    ProjectCumulativeCommitCount


from polaris.graphql.connection_utils import CountableConnection


class Project(
    # interface mixins
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    RepositoryCountResolverMixin,
    OrganizationRefResolverMixin,
    # Connection Mixins
    RepositoriesConnectionMixin,
    ContributorsConnectionMixin,
    RecentlyActiveRepositoriesConnectionMixin,
    # property mixins
    CumulativeCommitCountResolverMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorCount, RepositoryCount, OrganizationRef)
        named_node_resolver = ProjectNode
        interface_resolvers = {
            'CommitSummary': ProjectsCommitSummary,
            'ContributorCount': ProjectsContributorCount,
            'RepositoryCount': ProjectsRepositoryCount,
            'OrganizationRef': ProjectsOrganizationRef,
        }
        connection_node_resolvers = {
            'repositories': ProjectRepositoriesNodes,
            'contributors': ProjectContributorNodes,
            'recently_active_repositories': ProjectRecentlyActiveRepositoriesNodes,
        }
        selectable_field_resolvers = {
          'cumulative_commit_count': ProjectCumulativeCommitCount
        }
        connection_class = lambda: Projects



    @classmethod
    def resolve_field(cls, parent, info, project_key, **kwargs):
        return cls.resolve_instance(key=project_key, **kwargs)


class Projects(
    ActivityLevelSummaryResolverMixin,
    InceptionsResolverMixin,
    CountableConnection
):
    class Meta:
        node = Project
        summaries = (ActivityLevelSummary, graphene.List(InceptionsSummary))


class ProjectsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):

    projects = Project.ConnectionField()

    def resolve_projects(self, info, **kwargs):
        return Project.resolve_connection(
            self.get_connection_resolver_context('projects'),
            self.get_connection_node_resolver('projects'),
            self.get_instance_query_params(),
            **kwargs
        )


class RecentlyActiveProjectsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):

    recently_active_projects = Project.ConnectionField(days=graphene.Argument(
            graphene.Int,
            required=False,
            default_value=7,
            description="Return projects with commits within the specified number of days"
        ))

    def resolve_recently_active_projects(self, info, **kwargs):
        return Project.resolve_connection(
            self.get_connection_resolver_context('recently_active_projects'),
            self.get_connection_node_resolver('recently_active_projects'),
            self.get_instance_query_params(),
            **kwargs
        )
