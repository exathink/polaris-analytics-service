# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from datetime import datetime

from polaris.analytics.db.enums import  WorkItemType, WorkItemsStateType, WorkItemsStateFlowType, WorkItemsStateReleaseStatusType
from polaris.graphql.interfaces import NamedNode
from .utils import parse_json_timestamp

from polaris.common.enums import WorkTrackingIntegrationType as _WorkTrackingIntegrationType

WorkItemType = graphene.Enum.from_enum(WorkItemType)
WorkTrackingIntegrationType = graphene.Enum.from_enum(_WorkTrackingIntegrationType)
WorkItemsStateType = graphene.Enum.from_enum(WorkItemsStateType)
WorkItemsStateFlowType = graphene.Enum.from_enum(WorkItemsStateFlowType)
WorkItemsStateReleaseStatusType = graphene.Enum.from_enum(WorkItemsStateReleaseStatusType)

class FileTypesSummary(graphene.ObjectType):
    file_type = graphene.String(required=True)
    count = graphene.Int(required=True)


class WorkItemsSummary(graphene.ObjectType):
    key = graphene.String(required=True)
    name = graphene.String(required=True)
    work_item_type = graphene.String(required=True)
    display_id = graphene.String(required=True)
    url = graphene.String(required=True)
    state = graphene.String(required=False)
    state_type = graphene.String(required=False)


class CommitChangeStats(graphene.ObjectType):
    lines = graphene.Int(required=True)
    insertions = graphene.Int(required=True)
    deletions = graphene.Int(required=True)
    files = graphene.Int(required=True)


class ParentNodeRef(graphene.Interface):
    parent_name = graphene.String(required=False)
    parent_key = graphene.String(required=False)


class CommitInfo(NamedNode):
    commit_hash = graphene.String(required=True)
    repository = graphene.String(required=True)
    repository_key = graphene.String(required=True)
    repository_url = graphene.String(required=True)
    commit_date = graphene.DateTime(required=True)
    committer = graphene.String(required=True)
    committer_key = graphene.String(required=True)
    author_date = graphene.DateTime(required=True)
    author = graphene.String(required=True)
    author_key = graphene.String(required=True)
    commit_message = graphene.String(required=True)
    num_parents = graphene.Int(required=False)
    branch = graphene.String(required=False)
    stats = graphene.Field(CommitChangeStats, required=False)
    file_types_summary = graphene.Field(graphene.List(FileTypesSummary, required=False))
    integration_type = graphene.String(required=False)


class WorkItemCommitInfo(CommitInfo):
    commit_key = graphene.String(required=True)
    work_item_name = graphene.String(required=True)
    work_item_key = graphene.String(required=True)


class CommitTeamNodeRefs(graphene.Interface):
    author_team_name = graphene.String(required=False)
    author_team_key = graphene.String(required=False)
    committer_team_name = graphene.String(required=False)
    committer_team_key = graphene.String(required=False)


class EpicNodeRef(graphene.Interface):
    epic_name = graphene.String(required=False)
    epic_key = graphene.String(required=False)


class TeamNodeRef(graphene.Interface):
    team_name = graphene.String(required=False)
    team_key = graphene.String(required=False)
    capacity = graphene.Float(required=False)


class TeamNodeRefImpl(graphene.ObjectType):
    class Meta:
        interfaces = (TeamNodeRef,)


class TeamNodeRefs(graphene.Interface):
    team_node_refs = graphene.Field(graphene.List(TeamNodeRefImpl, required=False))


class WorkItemsSummaries(graphene.Interface):
    work_items_summaries = graphene.Field(graphene.List(WorkItemsSummary, required=False))


class CumulativeCommitCount(graphene.Interface):
    year = graphene.Int(required=True)
    week = graphene.Int(required=True)
    cumulative_commit_count = graphene.Int(required=True)


class WeeklyContributorCount(graphene.Interface):
    year = graphene.Int(required=True)
    week = graphene.Int(required=True)
    contributor_count = graphene.Int(required=True)


class CommitCount(NamedNode):
    commit_count = graphene.Int()


class CommitSummary(graphene.Interface):
    earliest_commit = graphene.DateTime(required=False)
    latest_commit = graphene.DateTime(required=False)
    commit_count = graphene.Int(required=False, default_value=0)


class ContributorCount(graphene.Interface):
    contributor_count = graphene.Int(required=False, default_value=0)


class CycleMetrics(graphene.Interface):
    lead_time = graphene.Float(required=False)
    cycle_time = graphene.Float(required=False)
    duration = graphene.Float(required=False)
    latency = graphene.Float(required=False)


class ContributorAliasInfo(graphene.Interface):
    key = graphene.String(required=True)
    name = graphene.String(required=True)
    alias = graphene.String(required=True)


class ContributorAliasInfoImpl(graphene.ObjectType):
    class Meta:
        interfaces = (ContributorAliasInfo, CommitSummary)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.latest_commit = parse_json_timestamp(self.latest_commit)
        self.earliest_commit = parse_json_timestamp(self.earliest_commit)


class ContributorAliasesInfo(graphene.Interface):
    contributor_aliases_info = graphene.Field(graphene.List(ContributorAliasInfoImpl, required=False))


class ProjectCount(graphene.Interface):
    project_count = graphene.Int(required=False, default_value=0)


class RepositoryCount(graphene.Interface):
    repository_count = graphene.Int(required=False, default_value=0)


class WorkItemsSourceCount(graphene.Interface):
    work_items_source_count = graphene.Int(required=False, default_value=0)


class OrganizationRef(graphene.Interface):
    organization_name = graphene.String(required=True)
    organization_key = graphene.String(required=True)


class ProjectRef(graphene.Interface):
    project_name = graphene.String(required=True)
    project_key = graphene.String(required=True)


# -- Project Settings related definitions
class FlowMetricsSettings:
    lead_time_target = graphene.Int(
        required=False,
        description="Target lead time in days",

    )
    cycle_time_target = graphene.Int(
        required=False,
        description="Target cycle time in days",

    )
    response_time_confidence_target = graphene.Float(
        required=False,
        description="The confidence level to which response times "
                    "(lead or cycle times) should be measured for predictability. Number between 0 and 1"
                    "If the SLA is 7 days cycle time with 80% confidence, response_time_confidence is 0.8."
                    "This setting is supported for backward compatibility but is deprecated. Use explicit"
                    "Lead and Cycle time confidence targets instead. See below.",

    )
    lead_time_confidence_target = graphene.Float(
        required=False,
        description="The target confidence level for lead time predictability measurement."
                    "If the SLA is 7 days lead time with 80% confidence, response_time_confidence is 0.8.",

    )
    cycle_time_confidence_target = graphene.Float(
        required=False,
        description="The target confidence level for cycle time predictability measurement."
                    "If the SLA is 7 days cycle time with 80% confidence, response_time_confidence is 0.8.",

    )
    include_sub_tasks = graphene.Boolean(
        required=False,
        description="To include or exclude sub-tasks in the metrics calculations",
        default_value=False
    )


class FlowMetricsSettingsImpl(FlowMetricsSettings, graphene.ObjectType):
    pass


class AnalysisPeriods:
    wip_analysis_period = graphene.Int(
        required=False,
        description="The default analysis window for closed work item metrics shown in the Wip Dashboard"
    )
    flow_analysis_period = graphene.Int(
        required=False,
        description="The default analysis window for closed work item metrics shown in the Flow Dashboard"
    )
    trends_analysis_period = graphene.Int(
        required=False,
        description="The default analysis window for closed work item metrics shown in the Trends Dashboard"
    )


class AnalysisPeriodsImpl(AnalysisPeriods, graphene.ObjectType):
    pass


class WipInspectorSettings:
    include_sub_tasks = graphene.Boolean(
        required=False,
        description="To include or exclude sub-tasks in the wip metrics calculations",
        default_value=True
    )


class WipInspectorSettingsImpl(WipInspectorSettings, graphene.ObjectType):
    pass

class ReleasesSettings:
    enable_releases = graphene.Boolean(
        required=False,
        description="Enable filtering value streams by release",
        default_value=False
    )

class ReleasesSettingsImpl(ReleasesSettings, graphene.ObjectType):
    pass

class CustomPhaseMapping:
    backlog = graphene.String(
        required=False,
        description="Display name of backlog phase"
    )

    open = graphene.String(
        required=False,
        description="Display name of open phase"
    )

    wip = graphene.String(
        required=False,
        description="Display name of wip phase"
    )

    complete = graphene.String(
        required=False,
        description="Display name of complete phase"
    )

    closed = graphene.String(
        required=False,
        description="Display name of closed phase"
    )

class CustomPhaseMappingImpl(CustomPhaseMapping, graphene.ObjectType):
    pass

class ProjectSettings(graphene.Interface):
    flow_metrics_settings = graphene.Field(FlowMetricsSettingsImpl, required=False)
    analysis_periods = graphene.Field(AnalysisPeriodsImpl, required=False)
    wip_inspector_settings = graphene.Field(WipInspectorSettingsImpl, required=False)
    releases_settings = graphene.Field(ReleasesSettingsImpl, required=False)
    custom_phase_mapping = graphene.Field(CustomPhaseMappingImpl, required=False)


class ProjectSettingsImpl(graphene.ObjectType):
    class Meta:
        interfaces = (ProjectSettings,)

    def __init__(self, *args, **kwargs):
        self.flow_metrics_settings = {}
        self.analysis_periods = {}
        self.wip_inspector_settings = {}
        self.releases_settings = {}
        self.custom_phase_mapping = {}

        super().__init__(*args, **kwargs)

        self.flow_metrics_settings = FlowMetricsSettingsImpl(**(self.flow_metrics_settings or {}))
        self.analysis_periods = AnalysisPeriodsImpl(**(self.analysis_periods or {}))
        self.wip_inspector_settings = WipInspectorSettingsImpl(**(self.wip_inspector_settings or {}))
        self.releases_settings = ReleasesSettingsImpl(**(self.releases_settings or {}))
        self.custom_phase_mapping = CustomPhaseMappingImpl(**(self.custom_phase_mapping or {}))

class ProjectInfo(graphene.Interface):
    settings = graphene.Field(ProjectSettingsImpl, required=False)

class ValueStreamInfo(graphene.Interface):
    description = graphene.String(required=False)
    work_item_selectors = graphene.List(graphene.String, required=True)

class ProjectSetupInfo(graphene.Interface):
    work_stream_count = graphene.Int(required=False)
    mapped_work_stream_count = graphene.Int(required=False)


class TeamSettings(ProjectSettings):
    pass


class TeamSettingsImpl(ProjectSettingsImpl):
    class Meta:
        interfaces = (TeamSettings,)


class TeamInfo(graphene.Interface):
    settings = graphene.Field(TeamSettingsImpl, required=False)


class WorkItemsSourceRef(graphene.Interface):
    work_items_source_name = graphene.String(required=True)
    work_items_source_key = graphene.String(required=True)
    work_tracking_integration_type = graphene.String(required=True)


class WorkItemInfo(graphene.Interface):
    work_item_key = graphene.String(required=True)
    work_item_type = graphene.String(required=True)
    display_id = graphene.String(required=True)
    url = graphene.String(required=True)
    description = graphene.String(required=False)
    state = graphene.String(required=True)
    created_at = graphene.DateTime(required=True)
    updated_at = graphene.DateTime(required=True)
    is_bug = graphene.Boolean(required=True)
    state_type = graphene.String(required=False)
    tags = graphene.String(required=False)
    priority = graphene.String(required=False)
    releases = graphene.List(graphene.String, required=False)
    story_points = graphene.Int(required=False)
    sprints = graphene.List(graphene.String, required=False)


class WorkItemStateTransition(graphene.Interface):
    event_date = graphene.DateTime(required=True)
    seq_no = graphene.Int(required=True)
    previous_state = graphene.String(required=False)
    previous_state_type = graphene.String(required=False)
    new_state = graphene.String(required=True)
    new_state_type = graphene.String(required=False)


class PullRequestInfo(graphene.Interface):
    display_id = graphene.String(required=True)
    state = graphene.String(required=True)
    created_at = graphene.DateTime(required=True)
    end_date = graphene.DateTime(required=False)
    age = graphene.Float(required=False)
    web_url = graphene.String(required=False)


class BranchRef(graphene.Interface):
    repository_name = graphene.String(required=True)
    repository_key = graphene.String(required=True)
    branch_name = graphene.String(required=True)


class Excluded(graphene.Interface):
    excluded = graphene.Boolean(required=False)


class ImplementationCost(graphene.Interface):
    budget = graphene.Float(required=False, description="Total engineering days estimated as per budget")
    effort = graphene.Float(required=False, description="Total engineering days required")
    duration = graphene.Float(required=False,
                              description="Span in days between earliest commit and latest commit")
    # TODO: Revisit whether we should separate this out from this interface
    # The calc involved here is substantially more expensive compared to the other two,
    # and unless we can make it comparable, we should do it.
    # right now, we are *not* returning this for the WorkItemStateDetails implementation
    # because it is so expensive.
    author_count = graphene.Int(required=False,
                                description="The number of distinct authors who committed to the work item")


class DeliveryCycleInfo(graphene.Interface):
    closed = graphene.Boolean(required=False)
    start_date = graphene.DateTime(required=False)
    end_date = graphene.DateTime(required=False)


class WorkItemStateTransitionImpl(graphene.ObjectType):
    class Meta:
        interfaces = (WorkItemStateTransition,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.event_date = datetime.strptime(self.event_date, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            self.event_date = datetime.strptime(self.event_date, "%Y-%m-%dT%H:%M:%S")


class WorkItemEventSpan(graphene.Interface):
    earliest_work_item_event = graphene.DateTime(required=False)
    latest_work_item_event = graphene.DateTime(required=False)


class PullRequestEventSpan(graphene.Interface):
    latest_pull_request_event = graphene.DateTime(required=False)


class WorkItemDaysInState(graphene.ObjectType):
    state = graphene.String(required=True)
    state_type = graphene.String(required=False)
    flow_type = graphene.String(required=False)
    days_in_state = graphene.Float(required=False)


class WorkItemStateDetail(graphene.ObjectType):
    class Meta:
        interfaces = (CommitSummary, ImplementationCost, DeliveryCycleInfo, CycleMetrics)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.latest_commit = parse_json_timestamp(self.latest_commit)
        self.earliest_commit = parse_json_timestamp(self.earliest_commit)
        self.start_date = parse_json_timestamp(self.start_date)
        self.end_date = parse_json_timestamp(self.end_date)

    current_state_transition = graphene.Field(WorkItemStateTransitionImpl, required=False)
    current_delivery_cycle_durations = graphene.List(WorkItemDaysInState, required=False)


class WorkItemStateDetails(graphene.Interface):
    work_item_state_details = graphene.Field(WorkItemStateDetail, required=False)


class AccountInfo(graphene.Interface):
    created = graphene.DateTime(required=False)
    updated = graphene.DateTime(required=False)


class ScopedRole(graphene.Interface):
    scope_key = graphene.String(required=True)
    role = graphene.String(required=True)


class UserInfo(graphene.Interface):
    name = graphene.String(required=True)
    first_name = graphene.String(required=True)
    last_name = graphene.String(required=True)
    email = graphene.String(required=True)


class ScopedRoleField(graphene.ObjectType):
    class Meta:
        interfaces = (ScopedRole, NamedNode)


class UserRoles(graphene.Interface):
    # this is to separate admin users from regular users
    system_roles = graphene.List(graphene.String)
    # this is to separate account owners and members for an account
    account_roles = graphene.List(ScopedRoleField)
    # this is to separate org owners and members for an org
    organization_roles = graphene.List(ScopedRoleField)


class OwnerInfo(graphene.Interface):
    owner_key = graphene.String(required=True)


class ArchivedStatus(graphene.Interface):
    """Indicate whether the implementing object is archived or not"""

    archived = graphene.Boolean(required=True)


class Describable(graphene.Interface):
    description = graphene.String(required=False)


class Enablement(graphene.Interface):
    enabled = graphene.Boolean(required=False)


class FeatureFlagInfo(graphene.Interface):
    enable_all = graphene.Boolean(required=True)
    active = graphene.Boolean(required=True)
    created = graphene.DateTime(required=True)


class FeatureFlagEnablementDetail(graphene.ObjectType):
    enabled = graphene.Boolean(required=False)
    scope = graphene.String(required=False)
    scope_key = graphene.String(required=False)
    scope_ref_name = graphene.String(required=False)


class FeatureFlagEnablements(graphene.Interface):
    enablements = graphene.List(FeatureFlagEnablementDetail, required=False)


class FeatureFlagScopeRef(graphene.Interface):
    scope_key = graphene.String(required=False)
    scope_ref_name = graphene.String(required=False)


class StateTypeAggregateMeasure(graphene.ObjectType):
    backlog = graphene.Float(required=False)
    open = graphene.Float(required=False)
    wip = graphene.Float(required=False)
    complete = graphene.Float(required=False)
    closed = graphene.Float(required=False)
    unmapped = graphene.Float(required=False)


class FunnelViewAggregateMetrics(graphene.Interface):
    work_item_state_type_counts = graphene.Field(StateTypeAggregateMeasure, required=False)

    total_effort_by_state_type = graphene.Field(StateTypeAggregateMeasure, required=False)

class Tags(graphene.Interface):
    tags = graphene.List(graphene.String, required=False)

class Releases(graphene.Interface):
    releases = graphene.List(graphene.String, required=False)

class StateMapping(graphene.ObjectType):
    state = graphene.String(required=True)
    state_type = graphene.String(required=False)
    flow_type = graphene.String(required=False)
    release_status = graphene.String(required=False)


class WorkItemStateMappings(graphene.Interface):
    work_item_state_mappings = graphene.Field(graphene.List(StateMapping))


class DevelopmentProgress(DeliveryCycleInfo):
    last_update = graphene.DateTime(required=False)
    elapsed = graphene.Float(required=False)


class DeliveryCycleSpan(graphene.Interface):
    earliest_closed_date = graphene.DateTime(required=False)
    latest_closed_date = graphene.DateTime(required=False)


class AggregateCycleMetrics(DeliveryCycleSpan):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=False)
    min_lead_time = graphene.Float(required=False)
    avg_lead_time = graphene.Float(required=False)
    max_lead_time = graphene.Float(required=False)

    min_cycle_time = graphene.Float(required=False)
    avg_cycle_time = graphene.Float(required=False)
    max_cycle_time = graphene.Float(required=False)
    q1_cycle_time = graphene.Float(required=False)
    median_cycle_time = graphene.Float(required=False)
    q3_cycle_time = graphene.Float(required=False)

    percentile_lead_time = graphene.Float(required=False)
    percentile_cycle_time = graphene.Float(required=False)
    target_percentile = graphene.Float(required=False)

    cadence = graphene.Int(required=False)

    # Implementation cost aggregates
    total_effort = graphene.Float(required=False)
    min_effort = graphene.Float(required=False)
    avg_effort = graphene.Float(required=False)
    max_effort = graphene.Float(required=False)
    percentile_effort = graphene.Float(required=False)

    min_duration = graphene.Float(required=False)
    avg_duration = graphene.Float(required=False)
    max_duration = graphene.Float(required=False)
    percentile_duration = graphene.Float(required=False)

    min_latency = graphene.Float(required=False)
    avg_latency = graphene.Float(required=False)
    max_latency = graphene.Float(required=False)
    percentile_latency = graphene.Float(required=False)

    work_items_in_scope = graphene.Int(required=False)
    work_items_with_commits = graphene.Int(required=False)
    work_items_with_null_cycle_time = graphene.Int(required=False)
    lead_time_target_percentile = graphene.Float(required=False)
    cycle_time_target_percentile = graphene.Float(required=False)


class TrendMeasurementImpl(graphene.ObjectType):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.measurement_date = datetime.strptime(self.measurement_date, "%Y-%m-%d")
        except ValueError:
            self.measurement_date = datetime.strptime(self.measurement_date, "%Y-%m-%dT%H:%M:%S")


class AggregateCycleMetricsImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (AggregateCycleMetrics,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        try:
            if self.earliest_closed_date is not None:
                self.earliest_closed_date = datetime.strptime(self.earliest_closed_date, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            self.earliest_closed_date = datetime.strptime(self.earliest_closed_date, "%Y-%m-%dT%H:%M:%S")

        try:
            if self.latest_closed_date is not None:
                self.latest_closed_date = datetime.strptime(self.latest_closed_date, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            self.latest_closed_date = datetime.strptime(self.latest_closed_date, "%Y-%m-%dT%H:%M:%S")


class CycleMetricsTrends(graphene.Interface):
    cycle_metrics_trends = graphene.List(AggregateCycleMetricsImpl)


class PipelineCycleMetrics(graphene.Interface):
    pipeline_cycle_metrics = graphene.Field(AggregateCycleMetricsImpl)


class ResponseTimeConfidence(graphene.Interface):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=True)

    lead_time_target = graphene.Float(required=False)
    lead_time_confidence = graphene.Float(required=False)

    cycle_time_target = graphene.Float(required=False)
    cycle_time_confidence = graphene.Float(required=False)


class ResponseTimeConfidenceImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (ResponseTimeConfidence,)


class ResponseTimeConfidenceTrends(graphene.Interface):
    response_time_confidence_trends = graphene.List(ResponseTimeConfidenceImpl)


class Traceability(graphene.Interface):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=True)
    traceability = graphene.Float(required=True)
    nospec_count = graphene.Int(required=True)
    spec_count = graphene.Int(required=True)
    total_commits = graphene.Int(required=True)


class TraceabilityImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (Traceability,)


class TraceabilityTrends(graphene.Interface):
    traceability_trends = graphene.List(TraceabilityImpl)


class FlowMixItem(graphene.ObjectType):
    category = graphene.String(required=True)
    sub_category_type = graphene.String(required=False)
    sub_category = graphene.String(required=False)

    work_item_count = graphene.Float(required=False)
    total_effort = graphene.Float(required=False)


class FlowMixMeasurement(graphene.Interface):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=True)
    flow_mix = graphene.Field(graphene.List(FlowMixItem), required=True)


class FlowMixMeasurementImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (FlowMixMeasurement,)

    def __init__(self, *args, **kwargs):
        self.flow_mix = []
        super().__init__(*args, **kwargs)

        self.flow_mix = [FlowMixItem(**item) for item in self.flow_mix if
                         item is not None and item['category'] is not None]


class FlowMixTrends(graphene.Interface):
    flow_mix_trends = graphene.List(FlowMixMeasurementImpl, required=True)


class CapacityMeasurement(graphene.Interface):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=True)
    # Core metrics
    total_commit_days = graphene.Float(required=False)
    # if this is specified then measurement is for an individual contributor over the period.
    contributor_key = graphene.String(required=False)
    contributor_name = graphene.String(required=False)

    avg_commit_days = graphene.Float(required=False)
    min_commit_days = graphene.Float(required=False)
    max_commit_days = graphene.Float(required=False)
    contributor_count = graphene.Int(required=False)


class CapacityMeasurementImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (CapacityMeasurement,)


class CapacityTrends(graphene.Interface):
    capacity_trends = graphene.List(CapacityMeasurementImpl, required=True)
    contributor_detail = graphene.List(CapacityMeasurementImpl, required=False)


class AggregatePullRequestMetrics(graphene.Interface):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=False)

    total_open = graphene.Int(required=False)
    total_closed = graphene.Int(required=False)

    min_age = graphene.Float(required=False)
    max_age = graphene.Float(required=False)
    avg_age = graphene.Float(required=False)
    percentile_age = graphene.Float(required=False)


class AggregatePullRequestMetricsImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (AggregatePullRequestMetrics,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class PipelinePullRequestMetrics(graphene.Interface):
    pipeline_pull_request_metrics = graphene.Field(AggregatePullRequestMetricsImpl)


class PullRequestMetricsTrends(graphene.Interface):
    pull_request_metrics_trends = graphene.List(AggregatePullRequestMetricsImpl)


class FlowRateMeasurement(graphene.Interface):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=True)
    arrival_rate = graphene.Int(required=False)
    close_rate = graphene.Int(required=False)


class FlowRateMeasurementImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (FlowRateMeasurement,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class FlowRateTrends(graphene.Interface):
    flow_rate_trends = graphene.List(FlowRateMeasurementImpl, required=True)


class ArrivalDepartureMeasurement(graphene.Interface):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=True)
    arrivals = graphene.Int(required=False)
    departures = graphene.Int(required=False)
    flowbacks = graphene.Int(required=False)
    passthroughs = graphene.Int(required=False)


class ArrivalDepartureRateMeasurementImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (ArrivalDepartureMeasurement,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class ArrivalDepartureTrends(graphene.Interface):
    arrival_departure_trends = graphene.List(ArrivalDepartureRateMeasurementImpl, required=True)


class BacklogMeasurement(graphene.Interface):
    measurement_date = graphene.Date(required=True)
    measurement_window = graphene.Int(required=True)
    backlog_size = graphene.Int(required=False)
    min_backlog_size = graphene.Int(required=False)
    max_backlog_size = graphene.Int(required=False)
    q1_backlog_size = graphene.Int(required=False)
    q3_backlog_size = graphene.Int(required=False)
    median_backlog_size = graphene.Int(required=False)
    avg_backlog_size = graphene.Int(required=False)


class BacklogMeasurementImpl(TrendMeasurementImpl):
    class Meta:
        interfaces = (BacklogMeasurement,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BacklogTrends(graphene.Interface):
    backlog_trends = graphene.List(BacklogMeasurementImpl, required=True)
