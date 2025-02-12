# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import Selectable, CountableConnection, ConnectionResolverMixin
from polaris.graphql.interfaces import NamedNode
from ..interfaces import ContributorCount, PipelineCycleMetrics, CycleMetricsTrends, CommitSummary, FlowMixTrends,\
    PullRequestMetricsTrends, CapacityTrends, TeamInfo, ArrivalDepartureTrends

from ..interface_mixins import NamedNodeResolverMixin, CycleMetricsTrendsResolverMixin, ArrivalDepartureTrendsResolverMixin, \
    PipelineCycleMetricsResolverMixin, FlowMixTrendsResolverMixin, PullRequestMetricsTrendsResolverMixin, \
    CapacityTrendsResolverMixin, TeamInfoResolverMixin

from .selectable import TeamNode, TeamContributorCount, TeamWorkItemDeliveryCycleNodes, \
    TeamCycleMetricsTrends, TeamPipelineCycleMetrics, TeamCommitNodes, TeamWorkItemNodes, \
    TeamPullRequestNodes, TeamCommitSummary, TeamFlowMixTrends, TeamPullRequestMetricsTrends, TeamCapacityTrends, \
    TeamArrivalDepartureTrends

from ..arguments import CycleMetricsTrendsParameters, CycleMetricsParameters, FlowMixTrendsParameters, \
    PullRequestMetricsTrendsParameters, CapacityTrendsParameters, ArrivalDepartureTrendsParameters

from ..work_item import WorkItemDeliveryCyclesConnectionMixin, WorkItemsConnectionMixin
from ..commit import CommitsConnectionMixin
from ..pull_request import PullRequestsConnectionMixin


class Team(
    NamedNodeResolverMixin,
    WorkItemDeliveryCyclesConnectionMixin,
    WorkItemsConnectionMixin,
    ArrivalDepartureTrendsResolverMixin,
    CycleMetricsTrendsResolverMixin,
    PipelineCycleMetricsResolverMixin,
    CommitsConnectionMixin,
    PullRequestsConnectionMixin,
    FlowMixTrendsResolverMixin,
    PullRequestMetricsTrendsResolverMixin,
    CapacityTrendsResolverMixin,
    TeamInfoResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, TeamInfo, ContributorCount, CycleMetricsTrends, ArrivalDepartureTrends,
                      PipelineCycleMetrics, CommitSummary,
                      FlowMixTrends, PullRequestMetricsTrends, CapacityTrends)
        named_node_resolver = TeamNode

        interface_resolvers = {
            'ContributorCount': TeamContributorCount,
            'PipelineCycleMetrics': TeamPipelineCycleMetrics,
            'CycleMetricsTrends': TeamCycleMetricsTrends,
            'ArrivalDepartureTrends': TeamArrivalDepartureTrends,
            'CommitSummary': TeamCommitSummary,
            'FlowMixTrends': TeamFlowMixTrends,
            'PullRequestMetricsTrends': TeamPullRequestMetricsTrends,
            'CapacityTrends': TeamCapacityTrends
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
            arrival_departure_trends_args=graphene.Argument(
                ArrivalDepartureTrendsParameters,
                required=False,
                description='Required when resolving WipArrivalRateTrends Interface'
            ),
            pull_request_metrics_trends_args=graphene.Argument(
                PullRequestMetricsTrendsParameters,
                required=False,
                description='Required when resolving PullRequestMetricsTrends interface'
            ),
            capacity_trends_args=graphene.Argument(
                CapacityTrendsParameters,
                required=False,
                description='Required when resolving CapacityTrends Interface'
            ),
            tags=graphene.Argument(
                graphene.List(graphene.String),
                required=False,
                description='Provide a list of tags to filter work_items by - this is currently only supported'
                            'for interface compatibility. Not implemented',
                default_value=None
            ),
            release=graphene.Argument(
                graphene.String,
                required=False,
                description='Provide a release to filter work_items by - this is currently only supported'
                            'for interface compatibility. Not implemented',
                default_value=None
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
        pull_request_metrics_trends_args=graphene.Argument(
            PullRequestMetricsTrendsParameters,
            required=False,
            description='Required when resolving PullRequestMetricsTrends interface'
        ),
        capacity_trends_args=graphene.Argument(
            CapacityTrendsParameters,
            required=False,
            description='Required when resolving CapacityTrends Interface'
        ),
    )

    def resolve_teams(self, info, **kwargs):
        return Team.resolve_connection(
            self.get_connection_resolver_context('teams'),
            self.get_connection_node_resolver('teams'),
            self.get_instance_query_params(),
            **kwargs
        )
