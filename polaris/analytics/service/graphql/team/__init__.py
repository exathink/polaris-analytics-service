# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import Selectable, CountableConnection, ConnectionResolverMixin
from polaris.graphql.interfaces import NamedNode
from ..interfaces import ContributorCount, PipelineCycleMetrics, CycleMetricsTrends, CommitSummary, FlowMixTrends
from ..interface_mixins import NamedNodeResolverMixin, CycleMetricsTrendsResolverMixin, \
    PipelineCycleMetricsResolverMixin, FlowMixTrendsResolverMixin
from .selectable import TeamNode, TeamContributorCount, TeamWorkItemDeliveryCycleNodes, \
    TeamCycleMetricsTrends, TeamPipelineCycleMetrics, TeamCommitNodes, TeamWorkItemNodes, \
    TeamPullRequestNodes, TeamCommitSummary, TeamFlowMixTrends

from ..arguments import CycleMetricsTrendsParameters, CycleMetricsParameters, FlowMixTrendsParameters
from ..work_item import WorkItemDeliveryCyclesConnectionMixin, WorkItemsConnectionMixin
from ..commit import CommitsConnectionMixin
from ..pull_request import PullRequestsConnectionMixin


class Team(
    NamedNodeResolverMixin,
    WorkItemDeliveryCyclesConnectionMixin,
    WorkItemsConnectionMixin,
    CycleMetricsTrendsResolverMixin,
    PipelineCycleMetricsResolverMixin,
    CommitsConnectionMixin,
    PullRequestsConnectionMixin,
    FlowMixTrendsResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, ContributorCount, CycleMetricsTrends, PipelineCycleMetrics, CommitSummary, FlowMixTrends)
        named_node_resolver = TeamNode

        interface_resolvers = {
            'ContributorCount': TeamContributorCount,
            'PipelineCycleMetrics': TeamPipelineCycleMetrics,
            'CycleMetricsTrends': TeamCycleMetricsTrends,
            'CommitSummary': TeamCommitSummary,
            'FlowMixTrends': TeamFlowMixTrends

        }

        connection_node_resolvers = {
            'work_item_delivery_cycles': TeamWorkItemDeliveryCycleNodes,
            'work_items': TeamWorkItemNodes,
            'commits': TeamCommitNodes,
            'pull_requests': TeamPullRequestNodes
        }
        connection_class = lambda: Teams

    @classmethod
    def Field(cls, **kwargs):
        return super().Field(
            key_is_required=False,
            contributor_count_days=graphene.Argument(
                graphene.Int,
                required=False,
                description="When evaluating contributor count "
                            "return only contributors that have committed code to the project in this many days"
            ),
            cycle_metrics_trends_args=graphene.Argument(
                CycleMetricsTrendsParameters,
                required=False,
                description='Required when resolving CycleMetricsTrends interface'
            ),
            pipeline_cycle_metrics_args=graphene.Argument(
                CycleMetricsParameters,
                required=False,
                description='Required when resolving PipelineCycleMetrics interface'
            ),
            flow_mix_trends_args=graphene.Argument(
                FlowMixTrendsParameters,
                required=False,
                description='Required when resolving FlowMixTrends Interface'
            ),
            **kwargs
        )

    @classmethod
    def resolve_field(cls, parent, info, team_key, **kwargs):
        return cls.resolve_instance(key=team_key, **kwargs)


class Teams(
    CountableConnection
):
    class Meta:
        node = Team


class TeamsConnectionMixin(ConnectionResolverMixin):
    teams = Team.ConnectionField(
        contributor_count_days=graphene.Argument(
            graphene.Int,
            required=False,
            description="When evaluating contributor count "
                        "return only contributors that have committed code to the project in this many days"
        ),
        cycle_metrics_trends_args=graphene.Argument(
            CycleMetricsTrendsParameters,
            required=False,
            description='Required when resolving CycleMetricsTrends interface'
        ),
        pipeline_cycle_metrics_args=graphene.Argument(
            CycleMetricsParameters,
            required=False,
            description='Required when resolving PipelineCycleMetrics interface'
        ),
        flow_mix_trends_args=graphene.Argument(
            FlowMixTrendsParameters,
            required=False,
            description='Required when resolving FlowMixTrends Interface'
        ),
    )

    def resolve_teams(self, info, **kwargs):
        return Team.resolve_connection(
            self.get_connection_resolver_context('teams'),
            self.get_connection_node_resolver('teams'),
            self.get_instance_query_params(),
            **kwargs
        )
