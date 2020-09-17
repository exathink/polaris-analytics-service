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
    WorkItemDaysInState, AggregateCycleMetricsImpl, TraceabilityImpl, WorkItemsSummary, ResponseTimeConfidenceImpl, ProjectSettingsImpl


class ContributorCountResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.contributor_count = None
        super().__init__(*args, **kwargs)

    def resolve_contributor_count(self, info, **kwargs):
        return 0 if self.contributor_count is None else self.contributor_count


class WorkItemsSummariesResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.work_items_summaries = []
        super().__init__(*args, **kwargs)

    def resolve_work_items_summaries(self, info, **kwargs):
        return [WorkItemsSummary(**summary) for summary in self.work_items_summaries if summary is not None]


class WorkItemStateTypeSummaryResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.work_item_state_type_counts = []
        super().__init__(*args, **kwargs)

    def resolve_work_item_state_type_counts(self, info, **kwargs):
        return StateTypeAggregateMeasure(**{
            result.get('state_type'): result['count']
            for result in self.work_item_state_type_counts if result is not None
        })


class WorkItemStateTypeSummaryResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.work_item_state_type_counts = []
        self.spec_state_type_counts = []
        super().__init__(*args, **kwargs)

    def resolve_work_item_state_type_counts(self, info, **kwargs):
        return StateTypeAggregateMeasure(**{
            result.get('state_type'): result['count']
            for result in self.work_item_state_type_counts if result is not None
        })

    def resolve_spec_state_type_counts(self, info, **kwargs):
        return StateTypeAggregateMeasure(**{
            result.get('state_type'): result['count']
            for result in self.spec_state_type_counts if result is not None
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
                ]
            )


class WorkItemStateMappingsResolverMixin(KeyIdResolverMixin):

    def __init__(self, *args, **kwargs):
        self.work_item_state_mappings = []
        super().__init__(*args, **kwargs)

    def resolve_work_item_state_mappings(self, info, **kwargs):
        return [
            StateMapping(**state_mapping)
            for state_mapping in self.work_item_state_mappings if state_mapping is not None
        ]


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