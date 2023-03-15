# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import abc
from datetime import datetime, timedelta
from io import StringIO

from sqlalchemy import and_, cast, Text, func, case, select, or_, literal, Date
from sqlalchemy.dialects.postgresql import ARRAY, array

from polaris.analytics.db.enums import WorkItemsStateType, FlowTypes, WorkItemTypesToFlowTypes
from polaris.analytics.db.model import work_items, work_item_delivery_cycles, work_items_sources
from polaris.analytics.service.graphql.utils import date_column_is_in_measurement_window, get_before_date
from polaris.common.enums import JiraWorkItemType
from polaris.graphql.base_classes import InterfaceResolver
from polaris.utils.exceptions import ProcessingException


def work_item_event_key_column(work_items, work_item_state_transitions):
    return (cast(work_items.c.key, Text) + ':' + cast(work_item_state_transitions.c.seq_no, Text)).label('key')


def work_item_commit_key_column(work_items, commits):
    return (cast(work_items.c.key, Text) + ':' + cast(commits.c.key, Text)).label('key')


def work_item_commit_name_column(work_items, commits):
    return (cast(work_items.c.display_id, Text) + ':' + cast(func.substr(commits.c.source_commit_id, 1, 8),
                                                             Text)).label('name')


def work_item_delivery_cycle_key_columns(work_items, work_item_delivery_cycles):
    return [
        work_item_delivery_cycles.c.delivery_cycle_id,
        (cast(work_items.c.key, Text) + ':' + cast(work_item_delivery_cycles.c.delivery_cycle_id, Text)).label('key'),
        work_item_delivery_cycles.c.work_item_id,
        work_items.c.work_items_source_id,
        work_items.c.parent_id,
    ]


def work_item_info_columns(work_items):
    return [
        work_items.c.key.label('work_item_key'),
        work_items.c.work_items_source_id,
        work_items.c.display_id,
        work_items.c.description,
        work_items.c.work_item_type,
        work_items.c.url,
        work_items.c.state,
        work_items.c.state_type,
        work_items.c.created_at,
        work_items.c.updated_at,
        work_items.c.is_bug,
        work_items.c.budget
    ]


def work_items_source_ref_info_columns(work_items_sources):
    return [
        work_items_sources.c.name.label('work_items_source_name'),
        work_items_sources.c.key.label('work_items_source_key'),
        work_items_sources.c.integration_type.label('work_tracking_integration_type')
    ]

def work_item_info_group_expr_columns(work_items):
    return [
        func.min(cast(work_items.c.key, Text)).label('work_item_key'),
        func.min(work_items.c.display_id).label('display_id'),
        func.min(work_items.c.description).label('description'),
        func.min(work_items.c.work_item_type).label('work_item_type'),
        func.min(work_items.c.url).label('url'),
        func.min(work_items.c.state).label('state'),
        func.min(work_items.c.state_type).label('state_type'),
        func.min(work_items.c.created_at).label('created_at'),
        func.min(work_items.c.updated_at).label('updated_at'),
        func.bool_or(work_items.c.is_bug).label('is_bug')
    ]


def work_item_event_columns(work_items, work_item_state_transitions):
    return [
        work_item_event_key_column(work_items, work_item_state_transitions),
        work_items.c.key.label('work_item_key'),
        work_items.c.name,
        work_items.c.display_id,
        work_items.c.description,
        work_items.c.work_item_type,
        work_items.c.url,
        work_items.c.state,
        work_items.c.state_type,
        work_items.c.created_at,
        work_items.c.updated_at,
        work_items.c.is_bug,
        work_item_state_transitions.c.seq_no,
        work_item_state_transitions.c.created_at.label('event_date'),
        work_item_state_transitions.c.previous_state,
        work_item_state_transitions.c.state.label('new_state')
    ]


def work_item_commit_info_columns(work_items, repositories, commits):
    return [
        work_item_commit_key_column(work_items, commits),
        work_item_commit_name_column(work_items, commits),
        commits.c.key.label('commit_key'),
        commits.c.source_commit_id.label('commit_hash'),
        work_items.c.name.label('work_item_name'),
        repositories.c.name.label('repository'),
        repositories.c.integration_type.label('integration_type'),
        repositories.c.url.label('repository_url'),
        repositories.c.key.label('repository_key'),
        commits.c.commit_date,
        commits.c.committer_contributor_name.label('committer'),
        commits.c.committer_contributor_key.label('committer_key'),
        commits.c.author_date,
        commits.c.author_contributor_name.label('author'),
        commits.c.author_contributor_key.label('author_key'),
        commits.c.commit_message,
        commits.c.num_parents,
        commits.c.created_on_branch.label('branch'),
        commits.c.stats,
        commits.c.source_file_types_summary.label('file_types_summary'),
        commits.c.work_items_summaries
    ]


def work_item_delivery_cycle_info_columns(work_items, work_item_delivery_cycles):
    return [
        *work_item_delivery_cycle_key_columns(work_items, work_item_delivery_cycles),
        work_items.c.name,
        work_item_delivery_cycles.c.start_date,
        work_item_delivery_cycles.c.end_date,
        case([(work_item_delivery_cycles.c.end_date != None, True)], else_=False).label('closed')
    ]


def work_items_connection_apply_time_window_filters(select_stmt, work_items, **kwargs):
    before = get_before_date(**kwargs)
    if 'days' in kwargs and kwargs['days'] > 0:
        window_start = before - timedelta(days=kwargs['days'])
        return select_stmt.where(
            and_(
                work_items.c.updated_at >= window_start,
                work_items.c.updated_at < before
            )
        )
    elif kwargs.get('before'):
        return select_stmt.where(
            work_items.c.updated_at < before
        )
    else:
        return select_stmt


def apply_active_only_filter(select_stmt, work_items, **kwargs):
    if 'active_only' in kwargs:
        select_stmt = select_stmt.where(
            work_items.c.state_type.in_([
                WorkItemsStateType.open.value,
                WorkItemsStateType.wip.value,
                WorkItemsStateType.complete.value
            ])
        )
        return select_stmt

def literal_postgres_string_array(string_array):
    # TODO:
    # we need this hack due to some obscure type conversions issues in
    # the ancient version of sqlalchemy we are using.
    # revert to using builtin functions when we upgrade
    output = StringIO()
    output.write("{")
    count = 0
    for item in string_array:
        if count > 0:
            output.write(",")

        output.write(f"\"{item}\"")
        count = count + 1

    output.write("}")
    return output.getvalue()

def apply_tags_clause(tags):
    return work_items.c.tags.op("&&")(literal_postgres_string_array(tags))

def apply_tags_filter(select_stmt, work_items, **kwargs):
    if 'tags' in kwargs and len(kwargs['tags']) > 0:
        select_stmt = select_stmt.where(
            apply_tags_clause(kwargs['tags'])
        )

    return select_stmt

def work_items_connection_apply_filters(select_stmt, work_items, **kwargs):
    select_stmt = work_items_connection_apply_time_window_filters(select_stmt, work_items, **kwargs)

    if 'state_types' in kwargs:
        select_stmt = select_stmt.where(work_items.c.state_type.in_(kwargs.get('state_types')))

    if 'defects_only' in kwargs:
        select_stmt = select_stmt.where(work_items.c.is_bug == True)

    if 'work_item_types' in kwargs:
        select_stmt = select_stmt.where(work_items.c.work_item_type.in_(kwargs.get('work_item_types')))

    if 'active_only' in kwargs:
        select_stmt = apply_active_only_filter(select_stmt, work_items, **kwargs)

    if 'tags' in kwargs:
        select_stmt = apply_tags_filter(select_stmt, work_items, **kwargs)

    if 'suppress_moved_items' not in kwargs or kwargs.get('suppress_moved_items') == True:
        select_stmt = select_stmt.where(work_items.c.is_moved_from_current_source != True)

    # This is true by default, so we include subtasks unless it is explicitly excluded.
    if kwargs.get('include_sub_tasks') == False:
        select_stmt = select_stmt.where(
            work_items.c.work_item_type != 'subtask'
        )

    # This is false by default, so we exclude epics unless it is explicitly requested.
    if kwargs.get('include_epics') is None or kwargs.get('include_epics') == False:
        select_stmt = select_stmt.where(
            work_items.c.is_epic == False
        )

    return select_stmt


def work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions, **kwargs):
    before = get_before_date(**kwargs)
    if 'days' in kwargs and kwargs['days'] > 0:
        window_start = before - timedelta(days=kwargs['days'])
        return select_stmt.where(
            and_(
                work_item_state_transitions.c.created_at >= window_start,
                work_item_state_transitions.c.created_at < before
            )
        )
    elif kwargs.get('before'):
        return select_stmt.where(
            work_item_state_transitions.c.created_at < before
        )
    else:
        return select_stmt


def apply_closed_within_days_filter(select_stmt, work_items, work_item_delivery_cycles, **kwargs):
    if 'closed_within_days' in kwargs:
        select_stmt = select_stmt.where(
            date_column_is_in_measurement_window(
                work_item_delivery_cycles.c.end_date,
                measurement_date=kwargs.get('closed_before') or datetime.utcnow(),
                measurement_window=kwargs['closed_within_days']
            )
        )
    return select_stmt


def apply_active_within_days_filter(select_stmt, work_items, work_item_delivery_cycles, **kwargs):
    if 'active_within_days' in kwargs:
        select_stmt = select_stmt.where(
            or_(
                work_item_delivery_cycles.c.end_date == None,
                date_column_is_in_measurement_window(
                    work_item_delivery_cycles.c.end_date,
                    measurement_date=datetime.utcnow(),
                    measurement_window=kwargs['active_within_days']
                )
            )
        )
    return select_stmt


def apply_specs_only_filter(select_stmt, work_items, work_item_delivery_cycles, **kwargs):
    if kwargs.get('specs_only'):
        if kwargs.get('include_epics'):
            select_stmt = select_stmt.where(
                or_(
                    # The notion of specs does not apply to epics only to cards
                    # we are simply going to allow all epics as specs if include epics is true
                    work_items.c.is_epic == True,
                    work_item_delivery_cycles.c.commit_count > 0,
                )
            )
        else:
            select_stmt = select_stmt.where(
                work_item_delivery_cycles.c.commit_count > 0
            )
    return select_stmt


def apply_defects_only_filter(select_stmt, work_items, **kwargs):
    if kwargs.get('defects_only'):
        select_stmt = select_stmt.where(
            work_items.c.is_bug == True
        )
    return select_stmt


def work_item_delivery_cycles_connection_apply_filters(select_stmt, work_items, work_item_delivery_cycles, **kwargs):
    select_stmt = apply_closed_within_days_filter(select_stmt, work_items, work_item_delivery_cycles, **kwargs)
    select_stmt = apply_active_within_days_filter(select_stmt,work_items, work_item_delivery_cycles, **kwargs)
    select_stmt = apply_specs_only_filter(select_stmt, work_items, work_item_delivery_cycles, **kwargs)

    return work_items_connection_apply_filters(select_stmt, work_items, **kwargs)


def work_items_cycle_metrics(**kwargs):
    closed_within_days = kwargs.get('closed_within_days')
    if closed_within_days is None:
        raise ProcessingException(
            "The argument 'closedWithinDays' must be specified when computing cycle metrics"
        )

    select_stmt = select([
        *work_items.columns,
        work_items.c.id.label('work_item_id'),
        work_item_delivery_cycles.c.delivery_cycle_id.label('delivery_cycle_id'),
        (func.min(work_item_delivery_cycles.c.lead_time) / (1.0 * 3600 * 24)).label('lead_time'),
        (func.min(work_item_delivery_cycles.c.spec_cycle_time) / (1.0 * 3600 * 24)).label('cycle_time'),
        func.min(work_item_delivery_cycles.c.commit_count).label('commit_count'),
        func.min(work_item_delivery_cycles.c.end_date).label('end_date'),
    ]).select_from(
        work_items.join(
            work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
        )
    )

    select_stmt = work_item_delivery_cycles_connection_apply_filters(
        select_stmt, work_items, work_item_delivery_cycles, **kwargs
    )

    return select_stmt.group_by(
        work_items.c.id,
        work_item_delivery_cycles.c.delivery_cycle_id
    )


class CycleMetricsTrendsBase(InterfaceResolver, abc.ABC):

    @classmethod
    def get_metrics_map(cls, cycle_metrics_trends_args, delivery_cycles_relation=work_item_delivery_cycles):
        return dict(
            # Cycle metrics
            cycle_metrics=dict(
                percentile_lead_time=func.percentile_disc(
                    cycle_metrics_trends_args.lead_time_target_percentile
                ).within_group(
                    delivery_cycles_relation.c.lead_time
                ).label(
                    'percentile_lead_time'
                ),
                percentile_cycle_time=func.percentile_disc(
                    cycle_metrics_trends_args.cycle_time_target_percentile
                ).within_group(
                    delivery_cycles_relation.c.spec_cycle_time
                ).label(
                    'percentile_cycle_time'
                ),
                min_lead_time=func.min(delivery_cycles_relation.c.lead_time).label('min_lead_time'),
                avg_lead_time=func.avg(delivery_cycles_relation.c.lead_time).label('avg_lead_time'),
                max_lead_time=func.max(delivery_cycles_relation.c.lead_time).label('max_lead_time'),
                min_cycle_time=func.min(delivery_cycles_relation.c.spec_cycle_time).label('min_cycle_time'),
                avg_cycle_time=func.avg(delivery_cycles_relation.c.spec_cycle_time).label('avg_cycle_time'),
                q1_cycle_time=func.percentile_disc(
                    0.25
                ).within_group(
                    delivery_cycles_relation.c.spec_cycle_time
                ).label(
                    'q1_cycle_time'
                ),
                median_cycle_time=func.percentile_disc(
                    0.50
                ).within_group(
                    delivery_cycles_relation.c.spec_cycle_time
                ).label(
                    'median_cycle_time'
                ),
                q3_cycle_time=func.percentile_disc(
                    0.75
                ).within_group(
                    delivery_cycles_relation.c.spec_cycle_time
                ).label(
                    'q3_cycle_time'
                ),
                max_cycle_time=func.max(delivery_cycles_relation.c.spec_cycle_time).label('max_cycle_time'),

            ),
            # Work item counts
            work_item_counts=dict(
                work_items_in_scope=func.count(
                    delivery_cycles_relation.c.delivery_cycle_id
                ).label('work_items_in_scope'),

                work_items_with_null_cycle_time=func.sum(
                    case([
                        (
                            and_(
                                # we need this and clause here, because we are
                                # counting a value that is None, but since this
                                # is invoked typically in a outerjoin, we need to
                                # qualify it with some non-null value. This implementation
                                # work correctly only for the closed items case, but this is
                                # the only case where it makes sense to compute this metric anyway.
                                delivery_cycles_relation.c.end_date != None,
                                delivery_cycles_relation.c.spec_cycle_time == None
                            ), 1
                        )
                    ], else_=0)
                ).label('work_items_with_null_cycle_time'),

                work_items_with_commits=func.sum(
                    case([
                        (
                            delivery_cycles_relation.c.commit_count > 0, 1
                        )
                    ], else_=0)
                ).label('work_items_with_commits'),
                cadence=func.count(cast(delivery_cycles_relation.c.end_date, Date).distinct()).label('cadence')
            ),
            # Implementation Complexity Metrics
            implementation_complexity=dict(
                total_effort=func.sum(delivery_cycles_relation.c.effort).label('total_effort'),
                min_effort=func.min(delivery_cycles_relation.c.effort).label('min_effort'),
                avg_effort=func.avg(delivery_cycles_relation.c.effort).label('avg_effort'),
                max_effort=func.max(delivery_cycles_relation.c.effort).label('max_effort'),
                percentile_effort=func.percentile_disc(
                    cycle_metrics_trends_args.latency_target_percentile
                ).within_group(
                    delivery_cycles_relation.c.effort
                ).label(
                    'percentile_effort'
                ),
                # latency
                min_latency=func.min(delivery_cycles_relation.c.latency / (1.0 * 3600 * 24)).label('min_latency'),
                avg_latency=func.avg(delivery_cycles_relation.c.latency / (1.0 * 3600 * 24)).label('avg_latency'),
                max_latency=func.max(delivery_cycles_relation.c.latency / (1.0 * 3600 * 24)).label('max_latency'),
                percentile_latency=func.percentile_disc(
                    cycle_metrics_trends_args.latency_target_percentile
                ).within_group(
                    delivery_cycles_relation.c.latency / (1.0 * 3600 * 24)
                ).label(
                    'percentile_latency'
                ),
                min_duration=(
                        func.min(
                            func.extract(
                                'epoch',
                                delivery_cycles_relation.c.latest_commit - delivery_cycles_relation.c.earliest_commit
                            )
                        ) / (1.0 * 3600 * 24)
                ).label('min_duration'),
                avg_duration=(
                        func.avg(
                            func.extract(
                                'epoch',
                                delivery_cycles_relation.c.latest_commit - delivery_cycles_relation.c.earliest_commit
                            )
                        ) / (1.0 * 3600 * 24)
                ).label('avg_duration'),
                max_duration=(
                        func.max(
                            func.extract(
                                'epoch',
                                delivery_cycles_relation.c.latest_commit - delivery_cycles_relation.c.earliest_commit
                            )
                        ) / (1.0 * 3600 * 24)
                ).label('max_duration'),
                percentile_duration=(
                        func.percentile_disc(
                            cycle_metrics_trends_args.duration_target_percentile
                        ).within_group(
                            func.extract(
                                'epoch',
                                delivery_cycles_relation.c.latest_commit - delivery_cycles_relation.c.earliest_commit
                            )
                        ) / (1.0 * 3600 * 24)
                ).label('percentile_duration'),

            )

        )

    @classmethod
    def get_metrics_columns(cls, cycle_metrics_trends_args, metrics_map, metric_type):
        metric_type_map = metrics_map[metric_type]
        return [
            metric_type_map[metric]
            for metric in cycle_metrics_trends_args.metrics if metric in metric_type_map
        ]

    @classmethod
    def get_cycle_metrics_json_object_columns(cls, cycle_metrics_trends_args, metrics_map, cycle_metrics_query):
        columns = []
        for metric in cycle_metrics_trends_args.metrics:
            if metric in metrics_map['cycle_metrics']:
                columns.extend([metric, (cycle_metrics_query.c[metric] / (1.0 * 24 * 3600)).label(metric)])
        return columns

    @staticmethod
    def get_work_item_count_metrics_json_object_columns(cycle_metrics_trends_args, metrics_map, cycle_metrics_query):
        columns = []
        for metric in cycle_metrics_trends_args.metrics:
            if metric in metrics_map['work_item_counts']:
                columns.extend([metric, cycle_metrics_query.c[metric]])
        return columns

    @staticmethod
    def get_implementation_complexity_metrics_json_object_columns(cycle_metrics_trends_args, metrics_map,
                                                                  cycle_metrics_query):
        columns = []
        for metric in cycle_metrics_trends_args.metrics:
            if metric in metrics_map['implementation_complexity']:
                columns.extend([metric, cycle_metrics_query.c[metric]])
        return columns

    @staticmethod
    def get_work_item_filter_clauses(interface_args, kwargs):

        clauses = []
        excluded_types = []

        if not interface_args.include_sub_tasks:
            # include subtasks is true by default, so this needs to be explicity overriden
            # if it is not to be added.
            excluded_types.append(JiraWorkItemType.sub_task.value)

        clauses.append(
            work_items.c.work_item_type.notin_(excluded_types)
        )

        if not interface_args.include_epics:
            # include_epics is false by default so this will normally be added
            clauses.append(work_items.c.is_epic == False)

        if interface_args.defects_only:
            clauses.append(
                work_items.c.is_bug == True
            )

        tags = kwargs.get('tags') or interface_args.get('tags')
        if tags and len(tags) > 0:
            clauses.append(apply_tags_clause(tags))

        return clauses

    @staticmethod
    def get_work_item_delivery_cycle_filter_clauses(cycle_metrics_trends_args):

        columns = []
        if cycle_metrics_trends_args.specs_only:
            columns.append(work_item_delivery_cycles.c.commit_count > 0)

        return columns


def map_work_item_type_to_flow_type(work_items):
    return case([
        # We need both a test for is_bug and for work_item_type
        # here, because in some providers like Github issues there is
        # no explicit issue type for bug. We use labels and other
        # attributes to set the is_bug flag and so that should always
        # override the work_item_type in determining if something is a bug.
        (
            work_items.c.is_bug,
            FlowTypes.defect.value
        ),
        (
            work_items.c.work_item_type.in_(
                WorkItemTypesToFlowTypes.defect_types
            ),
            FlowTypes.defect.value
        ),
        (
            work_items.c.work_item_type.in_(
                WorkItemTypesToFlowTypes.feature_types
            ),
            FlowTypes.feature.value
        ),
        (
            work_items.c.work_item_type.in_(
                WorkItemTypesToFlowTypes.task_types
            ),
            FlowTypes.task.value
        )

    ], else_=FlowTypes.other.value
    )