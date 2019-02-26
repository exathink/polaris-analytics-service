# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta
from sqlalchemy import and_

def work_item_info_columns(work_items):
    return [
        work_items.c.id,
        work_items.c.key,
        work_items.c.name,
        work_items.c.display_id,
        work_items.c.description,
        work_items.c.work_item_type,
        work_items.c.url,
        work_items.c.state,
        work_items.c.tags,
        work_items.c.created_at,
        work_items.c.updated_at
    ]


def work_item_event_columns(work_item_state_transitions):
    return [
        work_item_state_transitions.c.seq_no,
        work_item_state_transitions.c.created_at.label('event_date'),
        work_item_state_transitions.c.previous_state,
        work_item_state_transitions.c.state.label('new_state')
    ]


def work_items_connection_apply_time_window_filters(select_stmt, work_items,  **kwargs):
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


def work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions,  **kwargs):
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
