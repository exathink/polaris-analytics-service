# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from polaris.graphql.mixins import *
from datetime import datetime

from .interfaces import StateTypeAggregateMeasure, StateMapping, WorkItemStateTransitionImpl, WorkItemStateDetail, \
    WorkItemDaysInState, AggregateCycleMetricsImpl, TraceabilityImpl, WorkItemsSummary, ResponseTimeConfidenceImpl, \
    ProjectSettingsImpl, FlowMixMeasurementImpl, CapacityMeasurementImpl, AggregatePullRequestMetricsImpl, ArrivalDepartureRateMeasurementImpl, \
    ContributorAliasInfoImpl, FlowRateMeasurementImpl, BacklogMeasurementImpl, TeamNodeRefImpl, TeamSettingsImpl, ScopedRoleField


class ContributorCountResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.contributor_count = None
        super().__init__(*args, **kwargs)

    def resolve_contributor_count(self, info, **kwargs):
        return 0 if self.contributor_count is None else self.contributor_count


class ContributorAliasesInfoResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.contributor_aliases_info = []
        super().__init__(*args, **kwargs)

    def resolve_contributor_aliases_info(self, info, **kwargs):
        return [ContributorAliasInfoImpl(**alias_info) for alias_info in self.contributor_aliases_info if
                alias_info is not None]


class WorkItemsSummariesResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.work_items_summaries = []
        super().__init__(*args, **kwargs)

    def resolve_work_items_summaries(self, info, **kwargs):
        return [WorkItemsSummary(**summary) for summary in self.work_items_summaries if summary is not None]


class TeamNodeRefsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.team_node_refs = []
        super().__init__(*args, **kwargs)

    def resolve_team_node_refs(self, info, **kwargs):
        return [TeamNodeRefImpl(**ref) for ref in (self.team_node_refs or []) if
                ref is not None and ref.get('team_name') is not None]


class FlowMixTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.flow_mix_trends = []
        super().__init__(*args, **kwargs)

    def resolve_flow_mix_trends(self, info, **kwargs):
        return [FlowMixMeasurementImpl(**measurement) for measurement in self.flow_mix_trends if
                measurement is not None]


class WorkItemStateTypeSummaryResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.work_item_state_type_counts = []
        self.total_effort_by_state_type = []
        super().__init__(*args, **kwargs)

    def resolve_work_item_state_type_counts(self, info, **kwargs):
        return StateTypeAggregateMeasure(**{
            result.get('state_type'): result['count']
            for result in self.work_item_state_type_counts if result is not None
        })

    def resolve_total_effort_by_state_type(self, info, **kwargs):
        return StateTypeAggregateMeasure(**{
            result.get('state_type'): result['total_effort']
            for result in self.total_effort_by_state_type if result is not None
        })


class WorkItemStateDetailsResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.work_item_state_details = None
        super().__init__(*args, **kwargs)

    def resolve_work_item_state_details(self, info, **kwargs):
        if self.work_item_state_details is not None:
            return WorkItemStateDetail(
                current_state_transition=WorkItemStateTransitionImpl(
                    **self.work_item_state_details['current_state_transition']
                ),
                current_delivery_cycle_durations=[
                    WorkItemDaysInState(
                        **record
                    )
                    for record in self.work_item_state_details['current_delivery_cycle_durations']
                    if 'state' in record
                ],
                **self.work_item_state_details['commit_summary'],
                **self.work_item_state_details['implementation_cost'],
                **self.work_item_state_details['delivery_cycle_info'],
                **self.work_item_state_details['cycle_metrics']
            )

# This resolves the state mappings (plural) associated with the node - typically a work items sources
class WorkItemStateMappingsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.work_item_state_mappings = []
        super().__init__(*args, **kwargs)

    def resolve_work_item_state_mappings(self, info, **kwargs):
        return [
            StateMapping(**state_mapping)
            for state_mapping in self.work_item_state_mappings if state_mapping is not None
        ]

# This resolves the state mapping (singular) for the current state of a work item.
class WorkItemStateMappingResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.work_item_state_mapping = None
        super().__init__(*args, **kwargs)

    def resolve_work_item_state_mapping(self, info, **kwargs):
        return self.work_item_state_mapping


class CycleMetricsTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.cycle_metrics_trends = []
        super().__init__(*args, **kwargs)

    def resolve_cycle_metrics_trends(self, info, **kwargs):
        return [
            AggregateCycleMetricsImpl(**cycle_metrics)
            for cycle_metrics in self.cycle_metrics_trends or []
        ]


class PipelineCycleMetricsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.pipeline_cycle_metrics = None
        super().__init__(*args, **kwargs)

    def resolve_pipeline_cycle_metrics(self, info, **kwargs):
        return AggregateCycleMetricsImpl(**self.pipeline_cycle_metrics[0])


class TraceabilityTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.traceability_trends = []
        super().__init__(*args, **kwargs)

    def resolve_traceability_trends(self, info, **kwargs):
        return [
            TraceabilityImpl(**measurement)
            for measurement in self.traceability_trends or []
        ]


class ExcludedResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.excluded = False
        super().__init__(*args, **kwargs)

    def resolve_excluded(self, info, **kwargs):
        return self.excluded

class ResponseTimeConfidenceTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.response_time_confidence_trends = []
        super().__init__(*args, **kwargs)

    def resolve_response_time_confidence_trends(self, info, **kwargs):
        return [
            ResponseTimeConfidenceImpl(**measurement)
            for measurement in self.response_time_confidence_trends or []
        ]


class ProjectInfoResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.settings = {}
        super().__init__(*args, **kwargs)

    def resolve_settings(self, info, **kwargs):
        return ProjectSettingsImpl(**(self.settings if self.settings is not None else {}))


class TeamInfoResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.settings = {}
        super().__init__(*args, **kwargs)

    def resolve_settings(self, info, **kwargs):
        return TeamSettingsImpl(**(self.settings if self.settings is not None else {}))


class CapacityTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.capacity_trends = []
        self.contributor_detail = []
        super().__init__(*args, **kwargs)

    def resolve_capacity_trends(self, info, **kwargs):
        return [
            CapacityMeasurementImpl(**measurement)
            for measurement in self.capacity_trends or []
        ]

    def resolve_contributor_detail(self, info, **kwargs):
        return [
            CapacityMeasurementImpl(**measurement)
            for measurement in self.contributor_detail or []
        ]


class PipelinePullRequestMetricsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.pipeline_pull_request_metrics = None
        super().__init__(*args, **kwargs)

    def resolve_pipeline_pull_request_metrics(self, info, **kwargs):
        return AggregatePullRequestMetricsImpl(**self.pipeline_pull_request_metrics[0])


class PullRequestMetricsTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.pull_request_metrics_trends = []
        super().__init__(*args, **kwargs)

    def resolve_pull_request_metrics_trends(self, info, **kwargs):
        return [
            AggregatePullRequestMetricsImpl(**pull_request_metrics)
            for pull_request_metrics in self.pull_request_metrics_trends or []
        ]


class FlowRateTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.flow_rate_trends = []
        super().__init__(*args, **kwargs)

    def resolve_flow_rate_trends(self, info, **kwargs):
        return [
            FlowRateMeasurementImpl(**flow_rate_metrics)
            for flow_rate_metrics in self.flow_rate_trends or []
        ]

class ArrivalDepartureTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.arrival_departure_trends = []
        super().__init__(*args, **kwargs)

    def resolve_arrival_departure_trends(self, info, **kwargs):
        return [
            ArrivalDepartureRateMeasurementImpl(**arrival_departure_measurement)
            for arrival_departure_measurement in self.arrival_departure_trends or []
        ]


class BacklogTrendsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.backlog_trends = []
        super().__init__(*args, **kwargs)

    def resolve_backlog_trends(self, info, **kwargs):
        return [
            BacklogMeasurementImpl(**backlog_metrics)
            for backlog_metrics in self.backlog_trends or []
        ]


class UserRolesResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.system_roles = []
        self.organization_roles = []
        self.account_roles = []
        super().__init__(*args, **kwargs)

    def resolve_system_roles(self, info, **kwargs):
        return self.system_roles or []

    def resolve_organization_roles(self, info, **kwargs):
        return [
            ScopedRoleField(**scoped_roles)
            for scoped_roles in self.organization_roles or []
        ]

    def resolve_account_roles(self, info, **kwargs):
        return [
            ScopedRoleField(**scoped_roles)
            for scoped_roles in self.account_roles or []
        ]

class TagsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.tags = []
        super().__init__(*args, **kwargs)

    def resolve_tags(self, info, **kwargs):
        return self.tags or []

class ReleasesResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.releases = []
        super().__init__(*args, **kwargs)

    def resolve_releases(self, info, **kwargs):
        return sorted(self.releases, reverse=True) if self.releases else  []