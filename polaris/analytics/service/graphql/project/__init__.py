# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable

from ..interfaces import CommitSummary, ContributorCount, RepositoryCount, OrganizationRef
from ..interface_mixins import \
    NamedNodeResolverMixin, \
    CommitSummaryResolverMixin, \
    ContributorCountResolverMixin, \
    RepositoryCountResolverMixin, \
    OrganizationRefResolverMixin


from ..summaries import ActivityLevelSummary, InceptionsSummary
from ..summary_mixins import \
    ActivityLevelSummaryResolverMixin, \
    InceptionsResolverMixin

from ..repository import Repository
from ..contributor import Contributor

from .selectables import ProjectNode, \
    ProjectRepositoriesNodes, \
    ProjectContributorNodes, \
    ProjectsContributorCount, \
    ProjectsCommitSummary, \
    ProjectsRepositoryCount, \
    ProjectsOrganizationRef


from polaris.graphql.connection_utils import CountableConnection


class Project(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    RepositoryCountResolverMixin,
    OrganizationRefResolverMixin,
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
        connection_class = lambda: Projects


    # Child Fields
    repositories = Repository.ConnectionField()
    contributors = Contributor.ConnectionField()


    @classmethod
    def resolve_field(cls, parent, info, project_key, **kwargs):
        return cls.resolve_instance(key=project_key, **kwargs)

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            'project_repositories',
            ProjectRepositoriesNodes,
            self.get_instance_query_params(),
            **kwargs
        )

    def resolve_contributors(self, info, **kwargs):
        return Contributor.resolve_connection(
            'project_contributors',
            ProjectContributorNodes,
            self.get_instance_query_params(),
            level_of_detail='repository',
            apply_distinct=True,
            **kwargs
        )


class Projects(
    ActivityLevelSummaryResolverMixin,
    InceptionsResolverMixin,
    CountableConnection
):
    class Meta:
        node = Project
        summaries = (ActivityLevelSummary, graphene.List(InceptionsSummary))
