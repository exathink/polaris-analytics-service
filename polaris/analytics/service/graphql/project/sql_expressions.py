# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, func, literal, union_all, and_, or_
from polaris.analytics.db.enums import WorkItemsStateReleaseStatusType
from polaris.analytics.db.model import work_items, work_items_sources, work_item_delivery_cycles, \
    work_item_state_transitions, work_items_source_state_map
from ..work_item.sql_expressions import apply_specs_only_filter, work_items_connection_apply_filters, \
    work_item_delivery_cycles_connection_apply_filters
from polaris.common import db


def select_non_closed_work_items(project_nodes, select_columns, **kwargs):
    # first collect the non-closed items (the top of the funnel)
    non_closed_work_items_columns = [
        *select_columns,
        work_items.c.state.label('state'),
        func.coalesce(work_items.c.state_type, 'unmapped').label('state_type')
    ]

    non_closed_work_items = select(non_closed_work_items_columns).distinct().select_from(
        project_nodes.join(
            work_items_sources, work_items_sources.c.project_id == project_nodes.c.id,
        ).join(
            work_items, work_items.c.work_items_source_id == work_items_sources.c.id
        ).join(
            # here we only include the current delivery cycles.
            work_item_delivery_cycles,
            work_items.c.current_delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
        ).join(
            work_items_source_state_map,
            and_(
                work_items_source_state_map.c.work_items_source_id == work_items_sources.c.id,
                or_(
                    # we need to have this clause in here so that unmapped items are not dropped
                    # during this join. Note that since this causes multiple rows from the mapping
                    # to match for unmapped items we need to use a distinct() clause in the select
                    # to eliminate the duplicates
                    work_items.c.state_type == None,
                    work_items_source_state_map.c.state == work_items.c.state
                )
            )
        )
    ).where(
        and_(
            work_item_delivery_cycles.c.end_date == None,
            func.coalesce(work_items_source_state_map.c.release_status,
                          '') != WorkItemsStateReleaseStatusType.deferred.value
        )
    )
    # Check for specific filters for non closed items
    if kwargs.get('funnel_view_args') is not None:
        if kwargs.get('funnel_view_args').get('include_sub_tasks_in_non_closed_state') is not None:
            kwargs['include_sub_tasks'] = kwargs['funnel_view_args']['include_sub_tasks_in_non_closed_state']
    # apply work item filters
    non_closed_work_items = work_items_connection_apply_filters(
        non_closed_work_items,
        work_items,
        **kwargs
    )

    # apply the specs only filter for work_item_delivery_cycles. Note we cannot apply the
    # closed within days filter to open items since it will filter everything out.
    # that's why we are explicitly only including the specs_only _filter.
    non_closed_work_items = apply_specs_only_filter(
        non_closed_work_items,
        work_items,
        work_item_delivery_cycles,
        **kwargs
    )

    return non_closed_work_items


def select_closed_work_items(project_nodes, select_columns, **kwargs):
    # here we include all closed delivery cycles of a work item
    # so that we match the calculations for closed items flow metrics.
    closed_work_items_columns = [
        *select_columns,
        # we cannot use the work_item's state or state type here because
        # we need the state of the delivery cycle not the state
        # of the work item.
        work_item_state_transitions.c.state.label('state'),
        literal('closed').label('state_type'),
    ]
    closed_work_items = select(closed_work_items_columns).select_from(
        project_nodes.join(
            work_items_sources, work_items_sources.c.project_id == project_nodes.c.id,
        ).join(
            work_items, work_items.c.work_items_source_id == work_items_sources.c.id
        ).join(
            # This includes all closed delivery cycles of a work item so that we match
            # the calculations/counts for the Closed items metrics.
            work_item_delivery_cycles,
            work_item_delivery_cycles.c.work_item_id == work_items.c.id
        ).join(
            # we are joining to the state transitions here because we want to state of the
            # delivery cycle to reflect the resolution state of the delivery cycle (the state in
            # which it initially entered the closed phase.
            work_item_state_transitions,
            and_(
                work_item_state_transitions.c.work_item_id == work_item_delivery_cycles.c.work_item_id,
                work_item_state_transitions.c.seq_no == work_item_delivery_cycles.c.end_seq_no
            )
        ).join(
            work_items_source_state_map,
            and_(
                work_items_source_state_map.c.work_items_source_id == work_items_sources.c.id,
                # here we are filtering out any delivery cycle whose current state is deferred
                work_items_source_state_map.c.state == work_item_state_transitions.c.state
            )
        )
    ).where(
        and_(
            work_item_delivery_cycles.c.end_date != None,
            func.coalesce(work_items_source_state_map.c.release_status,'') != WorkItemsStateReleaseStatusType.deferred.value
        )
    )
    # Check for specific filters for closed items
    if kwargs.get('funnel_view_args') is not None:
        if kwargs.get('funnel_view_args').get('include_sub_tasks_in_closed_state') is not None:
            kwargs['include_sub_tasks'] = kwargs['funnel_view_args']['include_sub_tasks_in_closed_state']
    # Apply the standard filters for work items and work items delivery cycles here.
    # For closed items we apply the delivery cycle filters so that we match
    # the values that are calculated for closed item flow metrics.
    closed_work_items = work_item_delivery_cycles_connection_apply_filters(
        closed_work_items,
        work_items,
        work_item_delivery_cycles,
        **kwargs
    )
    return closed_work_items


def select_funnel_work_items(project_nodes, select_work_items_columns, **kwargs):
    # we need to strip out the state and state type column
    # from the input list if it is provided, since have custom logic
    # around how we show state type for the top of the funnel and the
    # bottom of the funnel.
    select_columns = [
        column
        for column in select_work_items_columns
        if column.name not in ['state_type', 'state']
    ]

    # top of funnel
    non_closed_work_items = select_non_closed_work_items(
        project_nodes,
        select_columns,
        **kwargs
    )

    # bottom of funnel
    closed_work_items = select_closed_work_items(
        project_nodes,
        select_columns,
        **kwargs
    )

    return union_all(
        closed_work_items,
        non_closed_work_items
    ).alias()
