# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable

from ..interfaces import CommitSummary, ContributorCount, ProjectCount, RepositoryCount
from ..mixins import \
    NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorCountResolverMixin, \
    ProjectCountResolverMixin, RepositoryCountResolverMixin

from ..project import Project
from ..repository import Repository
from ..contributor import Contributor

from .selectables import \
    OrganizationNode, OrganizationsCommitSummary, OrganizationsProjectCount, OrganizationsRepositoryCount,\
    OrganizationsContributorCount, OrganizationProjectsNodes, OrganizationRepositoriesNodes, OrganizationContributorNodes


from polaris.graphql.connection_utils import CountableConnection


class Organization(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    ProjectCountResolverMixin,
    RepositoryCountResolverMixin,
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
        connection_class = lambda: Organizations


    # Child Fields
    projects = Project.ConnectionField()
    repositories = Repository.ConnectionField()
    contributors = Contributor.ConnectionField()

    @classmethod
    def resolve_field(cls, parent, info, organization_key, **kwargs):
        return cls.resolve_instance(key=organization_key, **kwargs)

    def resolve_projects(self, info, **kwargs):
        return Project.resolve_connection(
            'organization_projects',
            OrganizationProjectsNodes,
            self.get_instance_query_params(),
            **kwargs
        )

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            'organization_repositories',
            OrganizationRepositoriesNodes,
            self.get_instance_query_params(),
            **kwargs
        )

    def resolve_contributors(self, info, **kwargs):
        return Contributor.resolve_connection(
            'organization_contributors',
            OrganizationContributorNodes,
            self.get_instance_query_params(),
            level_of_detail='repository',
            apply_distinct=True,
            **kwargs
        )

class Organizations(CountableConnection):
    class Meta:
        node = Organization





