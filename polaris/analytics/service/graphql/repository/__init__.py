# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from polaris.graphql.interfaces import NamedNode

from ..interfaces import CommitSummary, ContributorCount, OrganizationRef
from ..interface_mixins import KeyIdResolverMixin, \
    NamedNodeResolverMixin, CommitSummaryResolverMixin, \
    ContributorCountResolverMixin, OrganizationRefResolverMixin

from ..summaries import ActivityLevelSummary, InceptionsSummary
from ..summary_mixins import ActivityLevelSummaryResolverMixin, InceptionsResolverMixin
from ..selectable_field_mixins import CumulativeCommitCountResolverMixin

from .selectables import RepositoryNode, \
    RepositoriesCommitSummary, \
    RepositoryContributorNodes, \
    RepositoriesContributorCount, RepositoriesOrganizationRef, \
    RepositoryCommitNodes, \
    RepositoryCumulativeCommitCount

from ..contributor import ContributorsConnectionMixin
from ..commit import CommitsConnectionMixin

from polaris.graphql.connection_utils import CountableConnection


class Repository(
    # interface mixins
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    OrganizationRefResolverMixin,
    # connection mixins
    ContributorsConnectionMixin,
    CommitsConnectionMixin,
    # property mixins
    CumulativeCommitCountResolverMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorCount, OrganizationRef)
        named_node_resolver = RepositoryNode
        interface_resolvers = {
            'CommitSummary': RepositoriesCommitSummary,
            'ContributorCount': RepositoriesContributorCount,
            'OrganizationRef': RepositoriesOrganizationRef
        }
        connection_node_resolvers = {
            'contributors': RepositoryContributorNodes,
            'commits': RepositoryCommitNodes
        }
        selectable_field_resolvers = {
            'cumulative_commit_count': RepositoryCumulativeCommitCount
        }

        connection_class = lambda: Repositories

    @classmethod
    def resolve_field(cls, parent, info, repository_key, **kwargs):
        return cls.resolve_instance(repository_key, **kwargs)


class Repositories(
    ActivityLevelSummaryResolverMixin,
    InceptionsResolverMixin,
    CountableConnection
):
    class Meta:
        node = Repository
        summaries = (ActivityLevelSummary, graphene.List(InceptionsSummary))


class RepositoriesConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    repositories = Repository.ConnectionField()

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            self.get_connection_resolver_context('repositories'),
            self.get_connection_node_resolver('repositories'),
            self.get_instance_query_params(),
            **kwargs
        )


class RecentlyActiveRepositoriesConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    recently_active_repositories = Repository.ConnectionField(
        days=graphene.Argument(
            graphene.Int,
            required=False,
            default_value=7,
            description="Return repos with commits within the specified number of days"
        )
    )

    def resolve_recently_active_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            self.get_connection_resolver_context('recently_active_repositories'),
            self.get_connection_node_resolver('recently_active_repositories'),
            self.get_instance_query_params(),
            **kwargs
        )
