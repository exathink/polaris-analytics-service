# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from polaris.graphql.interfaces import NamedNode

from ..interfaces import CommitSummary, ContributorCount, OrganizationRef, Describable, PullRequestMetricsTrends
from ..interface_mixins import KeyIdResolverMixin, NamedNodeResolverMixin, PullRequestMetricsTrendsResolverMixin
from ..summaries import ActivityLevelSummary, InceptionsSummary
from ..summary_mixins import ActivityLevelSummaryResolverMixin, InceptionsResolverMixin
from ..selectable_field_mixins import CumulativeCommitCountResolverMixin, WeeklyContributorCountsResolverMixin
from ..arguments import PullRequestMetricsTrendsParameters

from .selectables import RepositoryNode, \
    RepositoriesCommitSummary, \
    RepositoryContributorNodes, \
    RepositoryRecentlyActiveContributorNodes, \
    RepositoryPullRequestNodes, \
    RepositoriesContributorCount, RepositoriesOrganizationRef, \
    RepositoryCommitNodes, \
    RepositoryCumulativeCommitCount, \
    RepositoryWeeklyContributorCount, \
    RepositoriesPullRequestMetricsTrends

from ..contributor import ContributorsConnectionMixin, RecentlyActiveContributorsConnectionMixin
from ..commit import CommitsConnectionMixin
from ..pull_request import PullRequestsConnectionMixin

from polaris.graphql.connection_utils import CountableConnection


class Repository(
    # interface mixins
    NamedNodeResolverMixin,
    # connection mixins
    ContributorsConnectionMixin,
    RecentlyActiveContributorsConnectionMixin,
    CommitsConnectionMixin,
    PullRequestsConnectionMixin,
    # field mixins
    CumulativeCommitCountResolverMixin,
    WeeklyContributorCountsResolverMixin,
    # interface mixins
    PullRequestMetricsTrendsResolverMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorCount, OrganizationRef, Describable, PullRequestMetricsTrends )
        named_node_resolver = RepositoryNode
        interface_resolvers = {
            'CommitSummary': RepositoriesCommitSummary,
            'ContributorCount': RepositoriesContributorCount,
            'OrganizationRef': RepositoriesOrganizationRef,
            'PullRequestMetricsTrends': RepositoriesPullRequestMetricsTrends
        }
        connection_node_resolvers = {
            'contributors': RepositoryContributorNodes,
            'recently_active_contributors': RepositoryRecentlyActiveContributorNodes,
            'commits': RepositoryCommitNodes,
            'pull_requests': RepositoryPullRequestNodes
        }
        selectable_field_resolvers = {
            'cumulative_commit_count': RepositoryCumulativeCommitCount,
            'weekly_contributor_counts': RepositoryWeeklyContributorCount,
        }

        connection_class = lambda: Repositories

    @classmethod
    def Field(cls, key_is_required=True, **kwargs):
        return super().Field(
            key_is_required,
            contributor_count_days=graphene.Argument(
                graphene.Int,
                required=False,
                description="When evaluating contributor count "
                            "return only contributors that have committed code to the project in this many days"
            ),
            pull_request_metrics_trends_args=graphene.Argument(
                PullRequestMetricsTrendsParameters,
                required=False,
                description='Required when resolving PullRequestMetricsTrends interface'
            ),
        )

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
        before=graphene.Argument(
            graphene.DateTime,
            required=False,
            description="End date of period to search for activity. If not specified it defaults to utc now"
        ),
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
