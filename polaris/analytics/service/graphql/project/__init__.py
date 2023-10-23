# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable, ConnectionResolverMixin

from ..interfaces import CommitSummary, ContributorCount, RepositoryCount, \
    OrganizationRef, ArchivedStatus, WorkItemEventSpan, FunnelViewAggregateMetrics, AggregateCycleMetrics, \
    CycleMetricsTrends, TraceabilityTrends, PipelineCycleMetrics, DeliveryCycleSpan, \
    ResponseTimeConfidenceTrends, ProjectInfo, FlowMixTrends, CapacityTrends, PipelinePullRequestMetrics, \
    PullRequestMetricsTrends, PullRequestEventSpan, FlowRateTrends, WipArrivalRateTrends, \
    BacklogTrends, ProjectSetupInfo, Tags, Releases

from ..interface_mixins import KeyIdResolverMixin, NamedNodeResolverMixin, \
    ContributorCountResolverMixin, WorkItemStateTypeSummaryResolverMixin, CycleMetricsTrendsResolverMixin, \
    TraceabilityTrendsResolverMixin, PipelineCycleMetricsResolverMixin, ResponseTimeConfidenceTrendsResolverMixin, \
    ProjectInfoResolverMixin, FlowMixTrendsResolverMixin, CapacityTrendsResolverMixin, \
    PipelinePullRequestMetricsResolverMixin, PullRequestMetricsTrendsResolverMixin, FlowRateTrendsResolverMixin, \
    BacklogTrendsResolverMixin, TagsResolverMixin, ReleasesResolverMixin, WipArrivalRateTrendsResolverMixin

from ..summaries import ActivityLevelSummary, InceptionsSummary
from ..summary_mixins import \
    ActivityLevelSummaryResolverMixin, \
    InceptionsResolverMixin

from ..selectable_field_mixins import \
    CumulativeCommitCountResolverMixin, \
    WeeklyContributorCountsResolverMixin

from ..repository import RepositoriesConnectionMixin, RecentlyActiveRepositoriesConnectionMixin
from ..contributor import ContributorsConnectionMixin, RecentlyActiveContributorsConnectionMixin
from ..commit import CommitsConnectionMixin
from ..work_items_source import WorkItemsSourcesConnectionMixin
from ..work_item import WorkItemsConnectionMixin, WorkItemEventsConnectionMixin, WorkItemCommitsConnectionMixin, \
    WorkItemDeliveryCyclesConnectionMixin, RecentlyActiveWorkItemsConnectionMixin
from ..pull_request import PullRequestsConnectionMixin
from ..value_stream import ValueStreamsConnectionMixin

from ..arguments import CycleMetricsTrendsParameters, CycleMetricsParameters, \
    TraceabilityMetricsTrendsParameters, ResponseTimeConfidenceTrendsParameters, \
    FlowMixTrendsParameters, CapacityTrendsParameters, PullRequestMetricsParameters, \
    PullRequestMetricsTrendsParameters, FlowRateTrendsParameters, BacklogTrendsParameters, \
    FunnelViewParameters, WipArrivalRateTrendsParameters

from .selectables import ProjectNode, \
    ProjectRepositoriesNodes, \
    ProjectContributorNodes, \
    ProjectCommitNodes, \
    ProjectWorkItemsSourceNodes, \
    ProjectRecentlyActiveWorkItemsNodes, \
    ProjectRecentlyActiveRepositoriesNodes, \
    ProjectRecentlyActiveContributorNodes, \
    ProjectPullRequestNodes,  \
    ProjectValueStreamNodes, \
    ProjectsContributorCount, \
    ProjectsDeliveryCycleSpan, \
    ProjectsCommitSummary, \
    ProjectsRepositoryCount, \
    ProjectsOrganizationRef, \
    ProjectsArchivedStatus, \
    ProjectCumulativeCommitCount, \
    ProjectWeeklyContributorCount, \
    ProjectCycleMetricsTrends, \
    ProjectPipelineCycleMetrics, \
    ProjectWorkItemEventSpan, \
    ProjectPullRequestEventSpan, \
    ProjectWorkItemNodes, \
    ProjectWorkItemEventNodes, \
    ProjectWorkItemCommitNodes, \
    ProjectFunnelViewAggregateMetrics, \
    ProjectCycleMetrics, \
    ProjectWorkItemDeliveryCycleNodes, \
    ProjectTraceabilityTrends, \
    ProjectResponseTimeConfidenceTrends, \
    ProjectsFlowMixTrends, \
    ProjectsCapacityTrends, \
    ProjectPipelinePullRequestMetrics, \
    ProjectPullRequestMetricsTrends, \
    ProjectFlowRateTrends, \
    ProjectWipArrivalRateTrends, \
    ProjectBacklogTrends, \
    ProjectsProjectSetupInfo, \
    ProjectTags, \
    ProjectReleases

from polaris.graphql.connection_utils import CountableConnection


class Project(
    # interface mixins
    NamedNodeResolverMixin,
    ProjectInfoResolverMixin,
    ContributorCountResolverMixin,
    WorkItemStateTypeSummaryResolverMixin,
    CycleMetricsTrendsResolverMixin,
    PipelineCycleMetricsResolverMixin,
    TraceabilityTrendsResolverMixin,
    ResponseTimeConfidenceTrendsResolverMixin,
    FlowMixTrendsResolverMixin,
    CapacityTrendsResolverMixin,
    PipelinePullRequestMetricsResolverMixin,
    PullRequestMetricsTrendsResolverMixin,
    FlowRateTrendsResolverMixin,
    WipArrivalRateTrendsResolverMixin,
    BacklogTrendsResolverMixin,
    TagsResolverMixin,
    ReleasesResolverMixin,

    # Connection Mixins
    RepositoriesConnectionMixin,
    ContributorsConnectionMixin,
    RecentlyActiveWorkItemsConnectionMixin,
    RecentlyActiveRepositoriesConnectionMixin,
    RecentlyActiveContributorsConnectionMixin,
    CommitsConnectionMixin,
    WorkItemsSourcesConnectionMixin,
    WorkItemsConnectionMixin,
    WorkItemEventsConnectionMixin,
    WorkItemCommitsConnectionMixin,
    WorkItemDeliveryCyclesConnectionMixin,
    PullRequestsConnectionMixin,
    ValueStreamsConnectionMixin,
    # field mixins
    CumulativeCommitCountResolverMixin,
    WeeklyContributorCountsResolverMixin,

    #
    Selectable
):
    class Meta:
        description = """
Project: A NamedNode representing a project. 
            
Implicit Interfaces: ArchivedStatus
"""
        interfaces = (
            # ----Implicit Interfaces ------- #
            NamedNode,
            ArchivedStatus,
            ProjectInfo,

            # ---- Explicit Interfaces -------#
            ProjectSetupInfo,
            CommitSummary,
            ContributorCount,
            DeliveryCycleSpan,
            RepositoryCount,
            OrganizationRef,
            WorkItemEventSpan,
            PullRequestEventSpan,
            FunnelViewAggregateMetrics,
            AggregateCycleMetrics,
            CycleMetricsTrends,
            PipelineCycleMetrics,
            TraceabilityTrends,
            ResponseTimeConfidenceTrends,
            FlowMixTrends,
            CapacityTrends,
            PipelinePullRequestMetrics,
            PullRequestMetricsTrends,
            FlowRateTrends,
            WipArrivalRateTrends,
            BacklogTrends,
            Tags,
            Releases
        )
        named_node_resolver = ProjectNode
        interface_resolvers = {
            'ProjectSetupInfo': ProjectsProjectSetupInfo,
            'CommitSummary': ProjectsCommitSummary,
            'ContributorCount': ProjectsContributorCount,
            'DeliveryCycleSpan': ProjectsDeliveryCycleSpan,
            'RepositoryCount': ProjectsRepositoryCount,
            'OrganizationRef': ProjectsOrganizationRef,
            'WorkItemEventSpan': ProjectWorkItemEventSpan,
            'PullRequestEventSpan': ProjectPullRequestEventSpan,
            'FunnelViewAggregateMetrics': ProjectFunnelViewAggregateMetrics,
            'AggregateCycleMetrics': ProjectCycleMetrics,
            'CycleMetricsTrends': ProjectCycleMetricsTrends,
            'PipelineCycleMetrics': ProjectPipelineCycleMetrics,
            'TraceabilityTrends': ProjectTraceabilityTrends,
            'ResponseTimeConfidenceTrends': ProjectResponseTimeConfidenceTrends,
            'FlowMixTrends': ProjectsFlowMixTrends,
            'CapacityTrends': ProjectsCapacityTrends,
            'PipelinePullRequestMetrics': ProjectPipelinePullRequestMetrics,
            'PullRequestMetricsTrends': ProjectPullRequestMetricsTrends,
            'FlowRateTrends': ProjectFlowRateTrends,
            'WipArrivalRateTrends': ProjectWipArrivalRateTrends,
            'BacklogTrends': ProjectBacklogTrends,
            'Tags': ProjectTags,
            'Releases': ProjectReleases
        }
        connection_node_resolvers = {
            'repositories': ProjectRepositoriesNodes,
            'contributors': ProjectContributorNodes,
            'recently_active_work_items': ProjectRecentlyActiveWorkItemsNodes,
            'recently_active_repositories': ProjectRecentlyActiveRepositoriesNodes,
            'recently_active_contributors': ProjectRecentlyActiveContributorNodes,
            'commits': ProjectCommitNodes,
            'work_items_sources': ProjectWorkItemsSourceNodes,
            'work_items': ProjectWorkItemNodes,
            'work_item_events': ProjectWorkItemEventNodes,
            'work_item_commits': ProjectWorkItemCommitNodes,
            'work_item_delivery_cycles': ProjectWorkItemDeliveryCycleNodes,
            'pull_requests': ProjectPullRequestNodes,
            'value_streams': ProjectValueStreamNodes,

        }
        selectable_field_resolvers = {
            'cumulative_commit_count': ProjectCumulativeCommitCount,
            'weekly_contributor_counts': ProjectWeeklyContributorCount,

        }
        connection_class = lambda: Projects

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
            closed_within_days=graphene.Argument(
                graphene.Int,
                required=False,
                description="When evaluating cycle metrics "
                            "calculate them over work items that have closed in this many prior days",
            ),
            cycle_metrics_target_percentile=graphene.Argument(
                graphene.Float,
                required=False,
                description="When evaluating cycle metrics "
                            "calculate the value at this percentile. For example: if we want the median value, the "
                            "the percentile is 0.5. Must be a number between 0 and 1",
                default_value=0.5
            ),
            defects_only=graphene.Argument(
                graphene.Boolean,
                required=False,
                description="When evaluating cycle metrics "
                            "include only defects"
            ),
            specs_only=graphene.Argument(
                graphene.Boolean,
                required=False,
                description="When evaluating cycle metrics "
                            "include only specs"
            ),
            include_sub_tasks=graphene.Argument(
                graphene.Boolean,
                required=False,
                description="When filtering work items under this node, include subtasks. The default value is True"
                            " so set this explicitly to false if you want to exclude subtasks ",
                default_value=True
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
            traceability_trends_args=graphene.Argument(
                TraceabilityMetricsTrendsParameters,
                required=False,
                description='Required when resolving TraceabilityTrends interface'
            ),
            response_time_confidence_trends_args=graphene.Argument(
                ResponseTimeConfidenceTrendsParameters,
                required=False,
                description='Required when resolving ResponseTimeConfidenceTrends interface'
            ),
            flow_mix_trends_args=graphene.Argument(
                FlowMixTrendsParameters,
                required=False,
                description='Required when resolving FlowMixTrends Interface'
            ),

            capacity_trends_args=graphene.Argument(
                CapacityTrendsParameters,
                required=False,
                description='Required when resolving CapacityTrends Interface'
            ),

            pipeline_pull_request_metrics_args=graphene.Argument(
                PullRequestMetricsParameters,
                required=False,
                description='Required when resolving PipelinePullRequestMetrics interface'
            ),

            pull_request_metrics_trends_args=graphene.Argument(
                PullRequestMetricsTrendsParameters,
                required=False,
                description='Required when resolving PullRequestMetricsTrends interface'
            ),

            flow_rate_trends_args=graphene.Argument(
                FlowRateTrendsParameters,
                required=False,
                description='Required when resolving FlowRateTrends interface'
            ),
            wip_arrival_rate_trends_args=graphene.Argument(
              WipArrivalRateTrendsParameters,
              required=False,
              description='Required when resolving WipArrivalRateTrends Interface'
            ),
            backlog_trends_args=graphene.Argument(
                BacklogTrendsParameters,
                required=False,
                description='Required when resolving BacklogTrends interface'
            ),

            funnel_view_args=graphene.Argument(
                FunnelViewParameters,
                required=False,
                description="Required when calculating metrics over both closed and non closed items"
            ),
            tags=graphene.Argument(
                graphene.List(graphene.String),
                required=False,
                description='Provide a list of tags to filter work_items by',
                default_value=None
            ),
            release=graphene.Argument(
                graphene.String,
                required=False,
                description='Provide a release to filter work_items by',
                default_value=None
            ),
            releases_active_within_days=graphene.Argument(
                graphene.Int,
                required=False,
                description='When fetching releases for a project include only releases where there'
                            'was a work item transition that occurred within the specified number of days. When None '
                            'all releases will be returned, so it is highly recommended that you provided a value here.',
                default_value=None
            ),

            **kwargs
        )

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
    projects = Project.ConnectionField(
        contributor_count_days=graphene.Argument(
            graphene.Int,
            required=False,
            default_value=30,
            description="When evaluating contributor count "
                        "return only contributors that have committed code to the project in this many days"
        ),
        cycle_metrics_trends_args=graphene.Argument(
            CycleMetricsTrendsParameters,
            required=False,
            description='Required when resolving CycleMetricsTrends interface'
        ),
        traceability_trends_args=graphene.Argument(
            TraceabilityMetricsTrendsParameters,
            required=False,
            default_value=True,
            description='Required when resolving TraceabilityTrends interface'
        ),
        flow_mix_trends_args=graphene.Argument(
            FlowMixTrendsParameters,
            required=False,
            description='Required when resolving FlowMixTrends Interface'
        ),
        commit_days_trends_args=graphene.Argument(
            CapacityTrendsParameters,
            required=False,
            description='Required when resolving CommitDaysTrends Interface'
        )
    )

    def resolve_projects(self, info, **kwargs):
        return Project.resolve_connection(
            self.get_connection_resolver_context('projects'),
            self.get_connection_node_resolver('projects'),
            self.get_instance_query_params(),
            **kwargs
        )


class RecentlyActiveProjectsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    recently_active_projects = Project.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime,
            required=False,
            description="End date of period to search for activity. If not specified it defaults to utc now"
        ),
        days=graphene.Argument(
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
