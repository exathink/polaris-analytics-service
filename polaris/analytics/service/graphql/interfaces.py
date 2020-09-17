# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from datetime import datetime

from polaris.analytics.db.enums import WorkItemsStateType
from polaris.graphql.interfaces import NamedNode

from polaris.common.enums import WorkTrackingIntegrationType as _WorkTrackingIntegrationType

WorkTrackingIntegrationType = graphene.Enum.from_enum(_WorkTrackingIntegrationType)
WorkItemsStateType = graphene.Enum.from_enum(WorkItemsStateType)


class FileTypesSummary(graphene.ObjectType):
    file_type = graphene.String(required=True)
    count = graphene.Int(required=True)


class WorkItemsSummary(graphene.ObjectType):
    key = graphene.String(required=True)
    name = graphene.String(required=True)
    work_item_type = graphene.String(required=True)
    display_id = graphene.String(required=True)
    url = graphene.String(required=True)


class CommitChangeStats(graphene.ObjectType):
    lines = graphene.Int(required=True)
    insertions = graphene.Int(required=True)
    deletions = graphene.Int(required=True)
    files = graphene.Int(required=True)


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

class FlowMetricsSettings(graphene.ObjectType):
    lead_time_target = graphene.Int(required=False)
    cycle_time_target = graphene.Int(required=False)
    response_time_confidence_target = graphene.Float(required=False)


class ProjectSettings(graphene.Interface):
    flow_metrics_settings = graphene.Field(FlowMetricsSettings, required=False)


class ProjectSettingsImpl(graphene.ObjectType):
    class Meta:
        interfaces = (ProjectSettings,)

    def __init__(self, *args, **kwargs):
        self.flow_metrics_settings = {}
        super().__init__(*args, **kwargs)

        self.flow_metrics_settings = FlowMetricsSettings(**(self.flow_metrics_settings or {}))


class ProjectInfo(graphene.Interface):
    settings = graphene.Field(ProjectSettingsImpl, required=False)


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


class WorkItemStateTransition(graphene.Interface):
    event_date = graphene.DateTime(required=True)
    seq_no = graphene.Int(required=True)
    previous_state = graphene.String(required=False)
    previous_state_type = graphene.String(required=False)
    new_state = graphene.String(required=True)
    new_state_type = graphene.String(required=False)


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


class WorkItemDaysInState(graphene.ObjectType):
    state = graphene.String(required=True)
    state_type = graphene.String(required=False)
    days_in_state = graphene.Float(required=False)


class WorkItemStateDetail(graphene.ObjectType):
    current_state_transition = graphene.Field(WorkItemStateTransitionImpl, required=False)
    current_delivery_cycle_durations = graphene.List(WorkItemDaysInState, required=False)


class WorkItemStateDetails(graphene.Interface):
    work_item_state_details = graphene.Field(WorkItemStateDetail, required=False)


class AccountInfo(graphene.Interface):
    created = graphene.DateTime(required=False)
    updated = graphene.DateTime(required=False)


class UserInfo(graphene.Interface):
    name = graphene.String(required=True)
    first_name = graphene.String(required=True)
    last_name = graphene.String(required=True)
    email = graphene.String(required=True)


class OwnerInfo(graphene.Interface):
    owner_key = graphene.String(required=True)


class ScopedRole(graphene.Interface):
    scope_key = graphene.String(required=True)
    role = graphene.String(required=True)


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


class WorkItemStateTypeCounts(graphene.Interface):
    work_item_state_type_counts = graphene.Field(StateTypeAggregateMeasure, required=False)
    spec_state_type_counts = graphene.Field(StateTypeAggregateMeasure, required=False)


class StateMapping(graphene.ObjectType):
    state = graphene.String(required=True)
    state_type = graphene.String(required=False)


class WorkItemStateMappings(graphene.Interface):
    work_item_state_mappings = graphene.Field(graphene.List(StateMapping))


class DeliveryCycleInfo(graphene.Interface):
    closed = graphene.Boolean(required=False)
    start_date = graphene.DateTime(required=False)
    end_date = graphene.DateTime(required=False)


class CycleMetrics(graphene.Interface):
    lead_time = graphene.Float(required=False)
    cycle_time = graphene.Float(required=False)


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

    # Implementation cost aggregates
    total_effort = graphene.Float(required=False)
    avg_duration = graphene.Float(required=False)
    percentile_duration = graphene.Float(required=False)

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


class ImplementationCost(graphene.Interface):
    effort = graphene.Float(required=False, description="Total engineering days required")
    duration = graphene.Float(required=False,
                              description="Span in days between earliest commit and latest commit")
    author_count = graphene.Int(required=False,
                                description="The number of distinct authors who committed to the work item")
