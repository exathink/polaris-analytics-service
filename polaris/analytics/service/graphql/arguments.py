# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene
from enum import Enum


class AggregateMetricsTrendsParameters(graphene.InputObjectType):
    before = graphene.Date(
        required=False,
        description="The end datetime of the period for which the trends are measured. Defaults to "
                    "datetime.utcnow() + 1 day"
    )
    days = graphene.Int(
        required=True,
        description="Days back from the before date for which trends should be measured. "
                    "Trends are reported for the dates drawn from the closed interval (R_start, R_end) where"
                    "R_start = before - days and R_end = before"
    )
    sampling_frequency = graphene.Int(
        required=False,
        description="Determines which dates within the reporting interval are chosen to report trend data. "
                    "This value K is specified in days. The reporting period (R_start, R_end) "
                    "is sampled at intervals of K days starting from "
                    "R_end and ending with R_start and aggregate metrics is reported for each such date. For example"
                    "If days = 30 and sampling_frequency is 7, then metrics are reported for dates ("
                    "before, -7, before -14, before - 21, before - 28) etc."
                    "The default value of the parameter is 1 day, but if you are reporting trends over larger period, "
                    " this "
                    "value can be used to reduce the number of data points returned by sampling less frequently over "
                    "the interval",
        default_value=1
    )
    measurement_window = graphene.Int(
        required=False,
        description="When measuring aggregate metrics like average_cycle_time, this parameter specifies how "
                    "many days back "
                    "from a sample date the underlying data set should be aggregated to compute the metric. "
                    "For example if"
                    "days = 30, sampling_frequency=7 and measurement_window=15, for each of the 4 dates in the window,"
                    "we will aggregate the underlying data set looking back 15 days from the sample date. "
                    "So we will be computing the average cycle times for the  work items that have "
                    "closed in the 15 days "
                    "prior to each sample date in the interval. "
                    ""
                    "This parameter is required when calculating metrics for closed work items, but not for active work items"
                    "since the set over which we are aggregating is defined by the state of the work items based on the before date"

    )


class CycleMetricsEnum(Enum):
    min_lead_time = 'min_lead_time'
    avg_lead_time = 'avg_lead_time'
    percentile_lead_time = 'percentile_lead_time'
    max_lead_time = 'max_lead_time'

    min_cycle_time = 'min_cycle_time'
    avg_cycle_time = 'avg_cycle_time'
    percentile_cycle_time = 'percentile_cycle_time'
    q1_cycle_time = 'q1_cycle_time'
    median_cycle_time = 'median_cycle_time'
    q3_cycle_time = 'q3_cycle_time'
    max_cycle_time = 'max_cycle_time'

    total_effort = 'total_effort'
    min_effort = 'min_effort'
    avg_effort = 'avg_effort'
    max_effort = 'max_effort'
    percentile_effort = 'percentile_effort'

    min_duration = 'min_duration'
    avg_duration = 'avg_duration'
    max_duration = 'max_duration'
    percentile_duration = 'percentile_duration'

    min_latency = 'min_latency'
    avg_latency = 'avg_latency'
    max_latency = 'max_latency'
    percentile_latency = 'percentile_latency'

    work_items_in_scope = 'work_items_in_scope'
    work_items_with_commits = 'work_items_with_commits'
    work_items_with_null_cycle_time = 'work_items_with_null_cycle_time'
    cadence = 'cadence'


class WorkItemTypeSelectionParameters(graphene.InputObjectType):
    include_epics = graphene.Boolean(
        required=False,
        description='Include epics in the cycle metrics analysis. Defaults to false',
        default_value=False
    )
    include_sub_tasks = graphene.Boolean(
        required=False,
        description='Include subtasks in the cycle metrics analysis. Defaults to true',
        default_value=True
    )
    defects_only = graphene.Boolean(
        required=False,
        description="Limit analysis to only defects. Defaults to false",
        default_value=False
    )
    specs_only = graphene.Boolean(
        required=False,
        description="Limit analysis to only specs (work_items with commit_count >0). Defaults to false",
        default_value=False
    )
    release = graphene.String(
        required=False,
        description="Limit analysis to only work items with the specified release",
    )
    tags = graphene.List(graphene.String,
        required=False,
        description="Limit analysis to only work items containing one or more of the specified tags"
    )



class CycleMetricsParameters(WorkItemTypeSelectionParameters, graphene.InputObjectType):
    metrics = graphene.List(
        graphene.Enum.from_enum(CycleMetricsEnum),
        required=True,
        description="Specify a list of the metrics that should be returned"
    )
    lead_time_target_percentile = graphene.Float(
        required=False,
        description="If percentile lead time is requested, then this specifies the target percentile value"
    )
    cycle_time_target_percentile = graphene.Float(
        required=False,
        description="If percentile cycle time is requested, then this specifies the target percentile value"
    )
    duration_target_percentile = graphene.Float(
        required=False,
        description="If percentile duration is requested, then this specifies the target percentile value"
    )
    latency_target_percentile = graphene.Float(
        required=False,
        description="If percentile latency is requested, then this specifies the target percentile value"
    )


class CycleMetricsTrendsParameters(AggregateMetricsTrendsParameters, CycleMetricsParameters):
    pass


class TraceabilityMetricsTrendsParameters(AggregateMetricsTrendsParameters):
    exclude_merges = graphene.Boolean(
        required=False,
        description="Limit analysis to non-merge commits only",
        default_value=False
    )


class ResponseTimeConfidenceTrendsParameters(AggregateMetricsTrendsParameters, WorkItemTypeSelectionParameters):
    lead_time_target = graphene.Int(
        required=True,
        description="Target lead time in days for which confidence is measured"
    )
    cycle_time_target = graphene.Int(
        required=True,
        description="Target cycle time in days for which confidence is measured"
    )


class FlowMixTrendsParameters(AggregateMetricsTrendsParameters, WorkItemTypeSelectionParameters):
    pass


class CapacityTrendsParameters(AggregateMetricsTrendsParameters):
    show_contributor_detail = graphene.Boolean(
        required=False,
        description="If this true then the results are broken down by contributor. Default = False",
        default_value=False
    )


class PullRequestMetricsEnum(Enum):
    total_open = 'total_open'
    total_closed = 'total_closed'
    min_age = 'min_age'
    max_age = 'max_age'
    avg_age = 'avg_age'
    percentile_age = 'percentile_age'


class PullRequestMetricsParameters(graphene.InputObjectType):
    metrics = graphene.List(
        graphene.Enum.from_enum(PullRequestMetricsEnum),
        required=True,
        description="Specify a list of the metrics that should be returned"
    )
    pull_request_age_target_percentile = graphene.Float(
        required=False,
        description="If percentile age is requested, then this specifies the target percentile value"
    )
    specs_only = graphene.Boolean(
        required=False,
        description="Only returns metrics for pull requests mapped to a card"
    )


class PullRequestMetricsTrendsParameters(AggregateMetricsTrendsParameters, PullRequestMetricsParameters):
    pass


class FlowRateMetricsEnum(Enum):
    arrival_rate = 'arrival_rate'
    close_rate = 'close_rate'


class FlowRateParameters(WorkItemTypeSelectionParameters, graphene.InputObjectType):
    metrics = graphene.List(
        graphene.Enum.from_enum(FlowRateMetricsEnum),
        required=True,
        description="Specify a list of the metrics that should be returned"
    )


class FlowRateTrendsParameters(AggregateMetricsTrendsParameters, FlowRateParameters, graphene.InputObjectType):
    pass


class BacklogMeasurementEnum(Enum):
    backlog_size = 'backlog_size'
    min_backlog_size = 'min_backlog_size'
    max_backlog_size = 'max_backlog_size'
    q1_backlog_size = 'q1_backlog_size'
    q3_backlog_size = 'q3_backlog_size'
    median_backlog_size = 'median_backlog_size'
    avg_backlog_size = 'avg_backlog_size'


class BacklogMeasurementParameters(WorkItemTypeSelectionParameters, graphene.InputObjectType):
    metrics = graphene.List(
        graphene.Enum.from_enum(BacklogMeasurementEnum),
        required=True,
        description="Specify a list of the metrics that should be returned"
    )


class BacklogTrendsParameters(AggregateMetricsTrendsParameters, BacklogMeasurementParameters):
    pass


class FunnelViewParameters(graphene.InputObjectType):
    include_sub_tasks_in_non_closed_state = graphene.Boolean(
        required=False,
        description='Include subtasks in metrics calculated over non closed items. Defaults to true',
        default_value=True
    )
    include_sub_tasks_in_closed_state = graphene.Boolean(
        required=False,
        description='Include subtasks in metrics calculated over closed items. Defaults to true',
        default_value=True
    )


class UserRolesParameters(graphene.InputObjectType):
    account_key = graphene.String(
        required=False,
        description="When fetching OrgRoles for the user, limit it to orgs from this account."
    )