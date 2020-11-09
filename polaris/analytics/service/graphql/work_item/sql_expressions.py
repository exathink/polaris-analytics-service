# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta
from sqlalchemy import and_, cast, Text, func, case, select, literal
from polaris.analytics.db.enums import WorkItemsStateType
from polaris.utils.exceptions import ProcessingException

from polaris.analytics.db.model import work_items, work_item_delivery_cycles, work_item_delivery_cycle_durations, \
    work_items_source_state_map


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
        work_items.c.is_bug
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
    before = None
    if 'before' in kwargs:
        before = kwargs['before']

    if 'days' in kwargs and kwargs['days'] > 0:
        if before:
            window_start = before - timedelta(days=kwargs['days'])
            return select_stmt.where(
                and_(
                    work_items.c.updated_at >= window_start,
                    work_items.c.updated_at <= before
                )
            )
        else:
            window_start = datetime.utcnow() - timedelta(days=kwargs['days'])
            return select_stmt.where(
                work_items.c.updated_at >= window_start
            )
    elif before:
        return select_stmt.where(
            work_items.c.updated_at <= before
        )
    else:
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
        select_stmt = select_stmt.where(
            work_items.c.state_type.in_([
                WorkItemsStateType.open.value,
                WorkItemsStateType.wip.value,
                WorkItemsStateType.complete.value
            ])
        )

    return select_stmt


def work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions, **kwargs):
    before = None
    if 'before' in kwargs:
        before = kwargs['before']

    if 'days' in kwargs and kwargs['days'] > 0:
        if before:
            window_start = before - timedelta(days=kwargs['days'])
            return select_stmt.where(
                and_(
                    work_item_state_transitions.c.created_at >= window_start,
                    work_item_state_transitions.c.created_at <= before
                )
            )
        else:
            window_start = datetime.utcnow() - timedelta(days=kwargs['days'])
            return select_stmt.where(
                work_item_state_transitions.c.created_at >= window_start
            )
    elif before:
        return select_stmt.where(
            work_item_state_transitions.c.created_at <= before
        )
    else:
        return select_stmt


def work_item_delivery_cycles_connection_apply_filters(select_stmt, work_items, work_item_delivery_cycles, **kwargs):
    if 'closed_within_days' in kwargs:
        measurement_date = datetime.utcnow().date()
        window_start = measurement_date - timedelta(days=kwargs.get('closed_within_days') - 1)
        select_stmt = select_stmt.where(
            work_item_delivery_cycles.c.end_date.between(window_start, measurement_date + timedelta(days=1))
        )

    if kwargs.get('specs_only'):
        select_stmt = select_stmt.where(
            work_item_delivery_cycles.c.commit_count > 0
        )

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
        (func.min(work_item_delivery_cycles.c.cycle_time) / (1.0 * 3600 * 24)).label('cycle_time'),
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
