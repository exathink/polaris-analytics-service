# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import logging
from polaris.common import db

from sqlalchemy import Column, Integer, select, and_, func, literal, or_, case, \
    extract, distinct, cast
from sqlalchemy.dialects.postgresql import UUID, insert, JSONB
from polaris.analytics.db.enums import WorkItemsStateType

from polaris.analytics.db.model import \
    work_items, commits, work_items_commits as work_items_commits_table, \
    work_item_state_transitions, work_item_delivery_cycles, work_item_delivery_cycle_durations, \
    work_item_delivery_cycle_contributors, contributor_aliases, work_items_source_state_map, \
    work_item_source_file_changes, source_files

logger = logging.getLogger('polaris.analytics.db.work_tracking')


def initialize_work_item_delivery_cycles(session, work_items_temp):
    session.connection().execute(
        work_item_delivery_cycles.insert().from_select([
            'work_item_id',
            'start_seq_no',
            'start_date',
            'end_seq_no',
            'end_date',
            'lead_time',
        ],
            select([
                work_items.c.id.label('work_item_id'),
                literal('0').label('start_seq_no'),
                work_items_temp.c.created_at.label('start_date'),
                case(
                    [
                        (
                            work_items_temp.c.state_type == WorkItemsStateType.closed.value,
                            1
                        )
                    ],
                    else_=None
                ).label('end_seq_no'),
                case(
                    [
                        (
                            work_items_temp.c.state_type == WorkItemsStateType.closed.value,
                            work_items_temp.c.updated_at.label('end_date')
                        )
                    ],
                    else_=None
                ),
                case(
                    [
                        (
                            work_items_temp.c.state_type == WorkItemsStateType.closed.value,
                            func.trunc((extract('epoch', work_items_temp.c.updated_at) -
                                        extract('epoch', work_items_temp.c.created_at))).label(
                                'lead_time')
                        )
                    ],
                    else_=None
                )
            ]).where(
                and_(
                    work_items_temp.c.key == work_items.c.key,
                    work_items_temp.c.work_item_id == None,
                )
            )
        )
    )


def update_work_item_delivery_cycles(session, work_items_temp):
    # Update end_date and lead_time in delivery cycles for work items transitioning to closed state
    # Setting end_date and end_seq_no at the first closed state transition
    session.connection().execute(
        work_item_delivery_cycles.update().values(
            end_seq_no=work_items.c.next_state_seq_no,
            end_date=work_items_temp.c.updated_at,
            lead_time=func.trunc((extract('epoch', work_items_temp.c.updated_at) -
                                  extract('epoch', work_item_delivery_cycles.c.start_date)))
        ).where(
            and_(
                work_items_temp.c.key == work_items.c.key,
                work_items.c.state != work_items_temp.c.state,
                work_item_delivery_cycles.c.work_item_id == work_items.c.id,
                work_item_delivery_cycles.c.delivery_cycle_id == work_items.c.current_delivery_cycle_id,
                work_items_temp.c.state_type == WorkItemsStateType.closed.value,
                work_items.c.state_type != WorkItemsStateType.closed.value
            )
        )
    )

    # create new delivery cycle when previous state_type is closed and new is non-closed
    session.connection().execute(
        work_item_delivery_cycles.insert().from_select([
            'work_item_id',
            'start_seq_no',
            'start_date',
        ],
            select([
                work_items.c.id.label('work_item_id'),
                work_items.c.next_state_seq_no.label('start_seq_no'),
                work_items_temp.c.updated_at.label('start_date'),
            ]).where(
                and_(
                    work_items_temp.c.key == work_items.c.key,
                    work_items.c.state != work_items_temp.c.state,
                    work_items.c.state_type == WorkItemsStateType.closed.value,
                    work_items_temp.c.state_type != WorkItemsStateType.closed.value,
                )
            )
        )
    )

    # Update current_delivery_cycle_id for all those transitioned from closed to non-closed state
    session.connection().execute(
        work_items.update().values(
            current_delivery_cycle_id=work_item_delivery_cycles.c.delivery_cycle_id
        ).where(
            and_(
                work_items.c.key == work_items_temp.c.key,
                work_item_delivery_cycles.c.work_item_id == work_items.c.id,
                or_(
                    work_item_delivery_cycles.c.delivery_cycle_id > work_items.c.current_delivery_cycle_id,
                    work_items.c.current_delivery_cycle_id == None  # Not an expected condition
                )
            )
        )
    )

    # Update delivery_cycle_durations for all work items with state changes
    update_work_item_delivery_cycle_durations(session, work_items_temp)

    # Recompute cycle time for all updated delivery cycles
    compute_work_item_delivery_cycles_cycle_time(session, work_items_temp)


def initialize_work_item_delivery_cycle_durations(session, work_items_temp):
    # Calculate the start and end date of each state transition from the state transitions table.
    work_items_state_time_spans = select([
        work_items.c.current_delivery_cycle_id,
        work_item_state_transitions.c.state,
        work_item_state_transitions.c.created_at.label('start_time'),
        work_item_state_transitions.c.seq_no,
        (func.lead(work_item_state_transitions.c.created_at).over(
            partition_by=work_item_state_transitions.c.work_item_id,
            order_by=work_item_state_transitions.c.seq_no
        )).label('end_time')
    ]).select_from(
        work_items_temp.join(
            work_items, work_items_temp.c.key == work_items.c.key
        ).join(
            work_item_state_transitions, work_item_state_transitions.c.work_item_id == work_items.c.id
        )
    ).where(
        work_items_temp.c.work_item_id == None
    ).alias()

    # compute the duration in each state from the time_span in each state.
    work_items_duration_in_state = select([
        work_items_state_time_spans.c.current_delivery_cycle_id,
        work_items_state_time_spans.c.state,
        work_items_state_time_spans.c.start_time,
        work_items_state_time_spans.c.end_time,
        (func.trunc(extract('epoch', work_items_state_time_spans.c.end_time) -
                    extract('epoch', work_items_state_time_spans.c.start_time))).label('duration')
    ]).select_from(
        work_items_state_time_spans
    ).alias()

    # aggregate the cumulative time in each state and insert into the delivery cycle durations table.
    session.connection().execute(
        work_item_delivery_cycle_durations.insert().from_select([
            'delivery_cycle_id',
            'state',
            'cumulative_time_in_state'
        ],

            select([
                (func.min(work_items_duration_in_state.c.current_delivery_cycle_id)).label('delivery_cycle_id'),
                work_items_duration_in_state.c.state.label('state'),
                (func.sum(work_items_duration_in_state.c.duration)).label('cumulative_time_in_state')
            ]).select_from(
                work_items_duration_in_state
            ).group_by(work_items_duration_in_state.c.current_delivery_cycle_id, work_items_duration_in_state.c.state)
        )
    )


def update_work_item_delivery_cycle_durations(session, work_items_temp):
    # For each work item that has a state transition, we recompute the
    # total time spent in each state during the current delivery cycle
    # and insert (or update) the rows in the delivery_cycle_durations table
    # for the current delivery cycle for those work items.

    # for the work items where the new state is not equal to current state
    # we need to recompute the time spans for each state transition.
    work_items_state_time_spans = select([
        work_items.c.current_delivery_cycle_id,
        work_item_state_transitions.c.state,
        work_item_state_transitions.c.created_at.label('start_time'),
        work_item_state_transitions.c.seq_no,
        (func.lead(work_item_state_transitions.c.created_at).over(
            partition_by=work_item_state_transitions.c.work_item_id,
            order_by=work_item_state_transitions.c.seq_no
        )).label('end_time')
    ]).select_from(
        work_items_temp.join(
            work_items, work_items_temp.c.key == work_items.c.key
        ).join(
            work_item_state_transitions, work_item_state_transitions.c.work_item_id == work_items.c.id
        ).join(
            work_item_delivery_cycles,
            work_item_delivery_cycles.c.delivery_cycle_id == work_items.c.current_delivery_cycle_id
        )
    ).where(
        and_(
            work_items_temp.c.state != work_items.c.state,
            work_item_state_transitions.c.created_at >= work_item_delivery_cycles.c.start_date
        )
    ).alias()

    # aggregate the total duration in each state
    work_items_durations_in_state = select([
        work_items_state_time_spans.c.current_delivery_cycle_id,
        work_items_state_time_spans.c.state,
        (
                extract('epoch', work_items_state_time_spans.c.end_time) -
                extract('epoch', work_items_state_time_spans.c.start_time)
        ).label('duration')
    ]).select_from(
        work_items_state_time_spans
    ).alias()

    # Insert or update rows in the delivery cycle durations table.
    upsert_stmt = insert(work_item_delivery_cycle_durations).from_select(
        [
            'delivery_cycle_id',
            'state',
            'cumulative_time_in_state'
        ],
        select([
            (func.min(work_items_durations_in_state.c.current_delivery_cycle_id)).label('delivery_cycle_id'),
            work_items_durations_in_state.c.state.label('state'),
            (func.sum(work_items_durations_in_state.c.duration)).label('cumulative_time_in_state')
        ]).select_from(
            work_items_durations_in_state
        ).group_by(work_items_durations_in_state.c.current_delivery_cycle_id, work_items_durations_in_state.c.state)
    )
    # Insert or update the existing durations.
    session.connection().execute(
        upsert_stmt.on_conflict_do_update(
            index_elements=['state', 'delivery_cycle_id'],
            set_=dict(cumulative_time_in_state=upsert_stmt.excluded.cumulative_time_in_state)
        )
    )


def compute_work_item_delivery_cycles_cycle_time(session, work_items_temp):
    delivery_cycles_cycle_time = select([
            work_items.c.current_delivery_cycle_id.label('current_delivery_cycle_id'),
            func.sum(case(
                [
                    (
                        or_(
                            work_items_source_state_map.c.state_type == WorkItemsStateType.open.value,
                            work_items_source_state_map.c.state_type == WorkItemsStateType.wip.value,
                            work_items_source_state_map.c.state_type == WorkItemsStateType.complete.value
                        ),
                        work_item_delivery_cycle_durations.c.cumulative_time_in_state
                    )
                ],
                else_=None
            )).label('cycle_time')
            ]).select_from(
            work_items_temp.join(
                work_items, work_items_temp.c.key == work_items.c.key
            ).join(
                work_item_delivery_cycles, work_item_delivery_cycles.c.delivery_cycle_id == work_items.c.current_delivery_cycle_id
            ).join(
                work_item_delivery_cycle_durations, work_item_delivery_cycle_durations.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
            ).join(
                work_items_source_state_map, work_items_source_state_map.c.work_items_source_id == work_items.c.work_items_source_id
            )).where(
                and_(work_item_delivery_cycle_durations.c.state == work_items_source_state_map.c.state,
                work_item_delivery_cycles.c.end_date != None)
            ).group_by(
                work_items.c.current_delivery_cycle_id
            ).cte('delivery_cycles_cycle_time')

    updated = session.connection().execute(
        work_item_delivery_cycles.update().where(
            work_item_delivery_cycles.c.delivery_cycle_id == delivery_cycles_cycle_time.c.current_delivery_cycle_id
        ).values(
            cycle_time=delivery_cycles_cycle_time.c.cycle_time
        )
    ).rowcount

    return dict(
        updated=updated
    )


def compute_work_item_delivery_cycle_commit_stats(session, work_items_temp):

    # select relevant rows to find various metrics
    delivery_cycles_commits_rows = select([
        work_item_delivery_cycles.c.delivery_cycle_id.label('delivery_cycle_id'),
        func.min(commits.c.commit_date).label('earliest_commit'),
        func.max(commits.c.commit_date).label('latest_commit'),
        func.count(distinct(commits.c.id)).label('commit_count'),
        func.count(distinct(commits.c.repository_id)).label('repository_count')
    ]).select_from(
        work_items_temp.join(
            work_items, work_items.c.key == work_items_temp.c.work_item_key
        ).join(
            work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
        ).join(
            work_items_commits_table, work_items_commits_table.c.work_item_id == work_items.c.id
        ).join(
            commits, work_items_commits_table.c.commit_id == commits.c.id
        )
    ).where(
        and_(
            work_item_delivery_cycles.c.start_date <= commits.c.commit_date,
            or_(
                work_item_delivery_cycles.c.end_date == None,
                commits.c.commit_date <= work_item_delivery_cycles.c.end_date
            )
        )
    ).group_by(
        work_item_delivery_cycles.c.delivery_cycle_id,

    ).cte('delivery_cycles_commits_rows')

    # update relevant work items delivery cycles with relevant metrics
    updated = session.connection().execute(
        work_item_delivery_cycles.update().where(
            work_item_delivery_cycles.c.delivery_cycle_id == delivery_cycles_commits_rows.c.delivery_cycle_id
        ).values(
            earliest_commit=delivery_cycles_commits_rows.c.earliest_commit,
            latest_commit=delivery_cycles_commits_rows.c.latest_commit,
            commit_count=delivery_cycles_commits_rows.c.commit_count,
            repository_count=delivery_cycles_commits_rows.c.repository_count,
        )
    ).rowcount

    return dict(
        updated=updated
    )


def update_work_items_commits_stats(session, organization_key, work_items_commits):
    # The following commit stats are calculated and updated for each work_item_delivery_cycle
    # 1. earliest_commit, latest_commit: earliest and latest commit for a work item delivery cycle
    # 2. repository_count: distinct repository count over all commits during a delivery cycle for a work item
    # 3. commit_count: distinct commit count over all commits during a delivery cycle for a work item

    updated = 0

    if len(work_items_commits) > 0:
        # create a temp table for received work item ids
        work_items_temp = db.create_temp_table(
            table_name='work_items_temp',
            columns=[
                Column('work_item_key', UUID(as_uuid=True)),
            ]
        )
        work_items_temp.create(session.connection(), checkfirst=True)

        # Get distinct work item keys from input
        distinct_work_items = []
        for entry in work_items_commits:
            if entry['work_item_key'] not in distinct_work_items:
                distinct_work_items.append(entry['work_item_key'])

        session.connection().execute(
            work_items_temp.insert().values(
                [
                    dict(
                        work_item_key=record
                    )
                    for record in distinct_work_items
                ]
            )
        )

        result = compute_work_item_delivery_cycle_commit_stats(session, work_items_temp)
        updated = result['updated']

    return dict(
        updated=updated
    )


def compute_implementation_complexity_metrics(session, work_items_temp):
    # The following metrics are calculated and updated for each work_item_delivery_cycle
    # 1. commit stats for non merge commits
    # 2. commit stats for merge commits

    updated = 0

    # select relevant rows to find various metrics
    delivery_cycles_commits_rows = select([
        work_item_delivery_cycles.c.delivery_cycle_id.label('delivery_cycle_id'),
        func.sum(
            case(
                [
                    (
                        commits.c.num_parents == 1,
                        cast(commits.c.stats["lines"].astext, Integer)
                    )
                ],
                else_=0
            )
        ).label('total_lines_changed_non_merge'),
        func.sum(
            case(
                [
                    (
                        commits.c.num_parents == 1,
                        cast(commits.c.stats["files"].astext, Integer)
                    )
                ],
                else_=0
            )
        ).label('total_files_changed_non_merge'),
        func.sum(
            case(
                [
                    (
                        commits.c.num_parents == 1,
                        cast(commits.c.stats["deletions"].astext, Integer)
                    )
                ],
                else_=0
            )
        ).label('total_lines_deleted_non_merge'),
        func.sum(
            case(
                [
                    (
                        commits.c.num_parents == 1,
                        cast(commits.c.stats["insertions"].astext, Integer)
                    )
                ],
                else_=0
            )
        ).label('total_lines_inserted_non_merge'),
        func.sum(
            case(
                [
                    (
                        commits.c.num_parents > 1,
                        cast(commits.c.stats["lines"].astext, Integer)
                    )
                ],
                else_=0
            )
        ).label('total_lines_changed_merge'),
        func.sum(
            case(
                [
                    (
                        commits.c.num_parents > 1,
                        cast(commits.c.stats["files"].astext, Integer)
                    )
                ],
                else_=0
            )
        ).label('total_files_changed_merge'),
        func.coalesce(func.trunc(func.avg(
            case(
                [
                    (
                        commits.c.num_parents > 1,
                        cast(commits.c.stats["lines"].astext, Integer)
                    )
                ],
                else_=None
            )
        )), 0).label('average_lines_changed_merge'),

    ]).select_from(
        work_items_temp.join(
            work_items, work_items.c.key == work_items_temp.c.work_item_key
        ).join(
            work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
        ).join(
            work_items_commits_table, work_items_commits_table.c.work_item_id == work_items.c.id
        ).join(
            commits, work_items_commits_table.c.commit_id == commits.c.id
        )
    ).where(
        and_(
            work_item_delivery_cycles.c.start_date <= commits.c.commit_date,
            or_(
                work_item_delivery_cycles.c.end_date == None,
                commits.c.commit_date <= work_item_delivery_cycles.c.end_date
            )
        )
    ).group_by(
        work_item_delivery_cycles.c.delivery_cycle_id,

    ).cte('delivery_cycles_commits_rows')

    # update relevant work items delivery cycles with relevant metrics
    updated = session.connection().execute(
        work_item_delivery_cycles.update().where(
            work_item_delivery_cycles.c.delivery_cycle_id == delivery_cycles_commits_rows.c.delivery_cycle_id
        ).values(
            total_lines_changed_non_merge=delivery_cycles_commits_rows.c.total_lines_changed_non_merge,
            total_files_changed_non_merge=delivery_cycles_commits_rows.c.total_files_changed_non_merge,
            total_lines_deleted_non_merge=delivery_cycles_commits_rows.c.total_lines_deleted_non_merge,
            total_lines_inserted_non_merge=delivery_cycles_commits_rows.c.total_lines_inserted_non_merge,
            total_lines_changed_merge=delivery_cycles_commits_rows.c.total_lines_changed_merge,
            total_files_changed_merge=delivery_cycles_commits_rows.c.total_files_changed_merge,
            average_lines_changed_merge=delivery_cycles_commits_rows.c.average_lines_changed_merge
        )
    ).rowcount
    return dict(
        updated=updated
    )


def compute_implementation_complexity_metrics_for_work_items(session, organization_key, work_items_commits):
    # The following metrics are calculated and updated for each work_item_delivery_cycle
    # 1. commit stats for non merge commits
    # 2. commit stats for merge commits

    updated = 0

    if len(work_items_commits) > 0:
        # create a temp table for received work item ids
        work_items_temp = db.create_temp_table(
            table_name='work_items_temp',
            columns=[
                Column('work_item_key', UUID(as_uuid=True)),
            ]
        )
        work_items_temp.create(session.connection(), checkfirst=True)

        # Get distinct work item keys from input
        distinct_work_items = []
        for entry in work_items_commits:
            if entry['work_item_key'] not in distinct_work_items:
                distinct_work_items.append(entry['work_item_key'])

        session.connection().execute(
            work_items_temp.insert().values(
                [
                    dict(
                        work_item_key=record
                    )
                    for record in distinct_work_items
                ]
            )
        )
        result = compute_implementation_complexity_metrics(session, work_items_temp)
        updated = result['updated']
    return dict(
        updated=updated
    )


def compute_implementation_complexity_metrics_for_commits(session, organization_key, commit_details):
    # This is called when we have commit keys as input
    # The following metrics are calculated and updated for each work_item_delivery_cycle
    # 1. commit stats for non merge commits
    # 2. commit stats for merge commits
    updated = 0

    if len(commit_details) > 0:
        # create a temp table for associated work item ids
        work_items_temp = db.create_temp_table(
            table_name='work_items_temp',
            columns=[
                Column('work_item_key', UUID(as_uuid=True)),
            ]
        )
        work_items_temp.create(session.connection(), checkfirst=True)

        # Get distinct work item keys from input
        distinct_commit_keys = []
        for entry in commit_details:
            if entry['key'] not in distinct_commit_keys:
                distinct_commit_keys.append(entry['key'])

        session.connection().execute(
            work_items_temp.insert().from_select(
                [
                    'work_item_key'
                ],
                select([
                    distinct(work_items.c.key)
                ]).select_from(
                    work_items.join(
                        work_items_commits_table, work_items.c.id == work_items_commits_table.c.work_item_id
                    ).join(
                        commits, commits.c.id == work_items_commits_table.c.commit_id
                    )
                ).where(
                    commits.c.key.in_(distinct_commit_keys)
                )
            )
        )
        result = compute_implementation_complexity_metrics(session, work_items_temp)
        updated = result['updated']
    return dict(
        updated=updated
    )


def compute_contributor_metrics(session, work_items_temp):
    # The following metrics are calculated and updated for each work_item_delivery_cycle_contributor
    # 1. total_lines_as_author
    # 2. total_lines_as_reviewer

    updated = 0

    # select relevant rows to find various metrics
    delivery_cycles_contributor_commits = select([
        work_item_delivery_cycles.c.delivery_cycle_id.label('delivery_cycle_id'),
        contributor_aliases.c.id.label('contributor_alias_id'),
        func.sum(
            case(
                [
                    (
                        and_(commits.c.num_parents == 1,
                             contributor_aliases.c.id == commits.c.author_contributor_alias_id),
                        cast(commits.c.stats["lines"].astext, Integer)
                    )
                ],
                else_=0
            )
        ).label('total_lines_as_author'),
        func.sum(
            case(
                [
                    (
                        or_(
                            and_(
                                commits.c.num_parents > 1,
                                contributor_aliases.c.id == commits.c.committer_contributor_alias_id
                            ),
                            and_(
                                commits.c.num_parents == 1,
                                contributor_aliases.c.id == commits.c.committer_contributor_alias_id,
                                commits.c.committer_contributor_alias_id != commits.c.author_contributor_alias_id
                            )
                        ),
                        cast(commits.c.stats["lines"].astext, Integer)
                    )
                ],
                else_=0
            )
        ).label('total_lines_as_reviewer'),
    ]).select_from(
        work_items_temp.join(
            work_items, work_items.c.key == work_items_temp.c.work_item_key
        ).join(
            work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
        ).join(
            work_items_commits_table, work_items_commits_table.c.work_item_id == work_items.c.id
        ).join(
            commits, work_items_commits_table.c.commit_id == commits.c.id
        ).join(
            contributor_aliases, or_(
                contributor_aliases.c.id == commits.c.author_contributor_alias_id,
                contributor_aliases.c.id == commits.c.committer_contributor_alias_id
            )
        )
    ).where(
        and_(
            work_item_delivery_cycles.c.start_date <= commits.c.commit_date,
            or_(
                work_item_delivery_cycles.c.end_date == None,
                commits.c.commit_date <= work_item_delivery_cycles.c.end_date
            )
        )
    ).group_by(
        work_item_delivery_cycles.c.delivery_cycle_id, contributor_aliases.c.id

    ).cte('delivery_cycles_contributor_commits')

    # upsert work items delivery cycle contributors with relevant metrics
    upsert_stmt = insert(work_item_delivery_cycle_contributors).from_select(
        ['delivery_cycle_id', 'contributor_alias_id', 'total_lines_as_author', 'total_lines_as_reviewer'],
        select(
            [
                delivery_cycles_contributor_commits.c.delivery_cycle_id,
                delivery_cycles_contributor_commits.c.contributor_alias_id,
                delivery_cycles_contributor_commits.c.total_lines_as_author,
                delivery_cycles_contributor_commits.c.total_lines_as_reviewer
            ]
        ).select_from(
           delivery_cycles_contributor_commits
        )

    )

    updated = session.connection().execute(
        upsert_stmt.on_conflict_do_update(
            index_elements=['delivery_cycle_id', 'contributor_alias_id'],
            set_=dict(
                total_lines_as_author=upsert_stmt.excluded.total_lines_as_author,
                total_lines_as_reviewer=upsert_stmt.excluded.total_lines_as_reviewer
            )
        )
    ).rowcount

    return dict(
        updated=updated
    )


def compute_contributor_metrics_for_work_items(session, organization_key, work_items_commits):
    updated = 0

    if len(work_items_commits) > 0:
        # create a temp table for received work item ids
        work_items_temp = db.create_temp_table(
            table_name='work_items_temp',
            columns=[
                Column('work_item_key', UUID(as_uuid=True)),
            ]
        )
        work_items_temp.create(session.connection(), checkfirst=True)

        # Get distinct work item keys from input
        distinct_work_items = []
        for entry in work_items_commits:
            if entry['work_item_key'] not in distinct_work_items:
                distinct_work_items.append(entry['work_item_key'])

        session.connection().execute(
            work_items_temp.insert().values(
                [
                    dict(
                        work_item_key=record
                    )
                    for record in distinct_work_items
                ]
            )
        )
        result = compute_contributor_metrics(session, work_items_temp)
        updated = result['updated']
    return dict(
        updated=updated
    )


def compute_contributor_metrics_for_commits(session, organization_key, commit_details):
    updated = 0

    # This is called when we have commit keys as input
    # The following metrics are calculated and updated for each work_item_delivery_cycle_contributor mapping
    # 1. total_lines_as_author
    # 2. total_lines_as_reviewer
    updated = 0

    if len(commit_details) > 0:
        # create a temp table for associated work item ids
        work_items_temp = db.create_temp_table(
            table_name='work_items_temp',
            columns=[
                Column('work_item_key', UUID(as_uuid=True)),
            ]
        )
        work_items_temp.create(session.connection(), checkfirst=True)

        # Get distinct work item keys from input
        distinct_commit_keys = []
        for entry in commit_details:
            if entry['key'] not in distinct_commit_keys:
                distinct_commit_keys.append(entry['key'])

        session.connection().execute(
            work_items_temp.insert().from_select(
                [
                    'work_item_key'
                ],
                select([
                    distinct(work_items.c.key)
                ]).select_from(
                    work_items.join(
                        work_items_commits_table, work_items.c.id == work_items_commits_table.c.work_item_id
                    ).join(
                        commits, commits.c.id == work_items_commits_table.c.commit_id
                    )
                ).where(
                    commits.c.key.in_(distinct_commit_keys)
                )
            )
        )
        result = compute_contributor_metrics(session, work_items_temp)
        updated = result['updated']
    return dict(
        updated=updated
    )


def populate_work_item_source_file_changes(session, commits_temp):
    updated = 0

    source_files_json = select([
        commits.c.id.label('commit_id'),
        func.jsonb_array_elements(commits.c.source_files).label('source_file')
    ]).select_from(
        commits_temp.join(
            commits, commits.c.key == commits_temp.c.commit_key
        )
    ).alias('source_files_json')

    source_file_changes = select([
        source_files_json.c.commit_id,
        source_files.c.id.label('source_file_id'),
        source_files.c.repository_id.label('repository_id'),
        cast(source_files_json.c.source_file, JSONB)['action'].label('file_action'),
        cast(cast(source_files_json.c.source_file, JSONB)['stats']['lines'].astext, Integer).label(
            'total_lines_changed'),
        cast(cast(source_files_json.c.source_file, JSONB)['stats']['deletions'].astext, Integer).label(
            'total_lines_deleted'),
        cast(cast(source_files_json.c.source_file, JSONB)['stats']['insertions'].astext, Integer).label(
            'total_lines_added')
    ]).select_from(
        source_files_json.join(
            source_files, cast(cast(source_files_json.c.source_file, JSONB)['key'].astext, UUID) == source_files.c.key
        )
    ).alias('source_file_changes')

    # Create a temp table to store data for commits within delivery cycles
    work_item_source_file_changes_temp = db.temp_table_from(
        work_item_source_file_changes,
        table_name='work_item_source_file_changes_temp',
        exclude_columns=[
            work_item_source_file_changes.c.id
        ]
    )
    work_item_source_file_changes_temp.create(session.connection(), checkfirst=True)

    # select relevant rows and insert into temp table
    session.connection().execute(
        work_item_source_file_changes_temp.insert().from_select([
            'commit_id',
            'work_item_id',
            'delivery_cycle_id',
            'repository_id',
            'source_file_id',
            'source_commit_id',
            'commit_date',
            'committer_contributor_alias_id',
            'author_contributor_alias_id',
            'num_parents',
            'created_on_branch',
            'file_action',
            'total_lines_changed',
            'total_lines_deleted',
            'total_lines_added'
        ],
            select([
                commits.c.id.label('commit_id'),
                work_items.c.id.label('work_item_id'),
                work_item_delivery_cycles.c.delivery_cycle_id.label('delivery_cycle_id'),
                source_file_changes.c.repository_id.label('repository_id'),
                source_file_changes.c.source_file_id.label('source_file_id'),
                commits.c.source_commit_id,
                commits.c.commit_date,
                commits.c.committer_contributor_alias_id,
                commits.c.author_contributor_alias_id,
                commits.c.num_parents,
                commits.c.created_on_branch,
                source_file_changes.c.file_action.label('file_action'),
                source_file_changes.c.total_lines_changed.label('total_lines_changed'),
                source_file_changes.c.total_lines_deleted.label('total_lines_deleted'),
                source_file_changes.c.total_lines_added.label('total_lines_added')
            ]).select_from(
                commits_temp.join(
                    commits, commits_temp.c.commit_key == commits.c.key
                ).join(
                    work_items_commits_table, commits.c.id == work_items_commits_table.c.commit_id
                ).join(
                    work_items, work_items.c.id == work_items_commits_table.c.work_item_id
                ).join(
                    work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
                ).join(
                    source_file_changes, source_file_changes.c.commit_id == commits.c.id
                )
            ).where(
                and_(
                    commits.c.commit_date >= work_item_delivery_cycles.c.start_date,
                    or_(
                        work_item_delivery_cycles.c.end_date == None,
                        commits.c.commit_date <= work_item_delivery_cycles.c.end_date
                    )
                )
            )
        )
    )

    # Find commits outside all delivery cycles
    session.connection().execute(
        work_item_source_file_changes_temp.insert().from_select([
            'commit_id',
            'work_item_id',
            'delivery_cycle_id',
            'repository_id',
            'source_file_id',
            'source_commit_id',
            'commit_date',
            'committer_contributor_alias_id',
            'author_contributor_alias_id',
            'num_parents',
            'created_on_branch',
            'file_action',
            'total_lines_changed',
            'total_lines_deleted',
            'total_lines_added'
        ],
            select([
                commits.c.id.label('commit_id'),
                work_items.c.id.label('work_item_id'),
                literal(None).label('delivery_cycle_id'),
                source_file_changes.c.repository_id.label('repository_id'),
                source_file_changes.c.source_file_id.label('source_file_id'),
                commits.c.source_commit_id,
                commits.c.commit_date,
                commits.c.committer_contributor_alias_id,
                commits.c.author_contributor_alias_id,
                commits.c.num_parents,
                commits.c.created_on_branch,
                source_file_changes.c.file_action.label('file_action'),
                source_file_changes.c.total_lines_changed.label('total_lines_changed'),
                source_file_changes.c.total_lines_deleted.label('total_lines_deleted'),
                source_file_changes.c.total_lines_added.label('total_lines_added')
            ]).select_from(
                commits_temp.join(
                    commits, commits_temp.c.commit_key == commits.c.key
                ).join(
                    work_items_commits_table, commits.c.id == work_items_commits_table.c.commit_id
                ).join(
                    work_items, work_items.c.id == work_items_commits_table.c.work_item_id
                ).outerjoin(
                    work_item_source_file_changes_temp,
                    and_(
                        commits.c.id == work_item_source_file_changes_temp.c.commit_id,
                        work_items.c.id == work_item_source_file_changes_temp.c.work_item_id
                    )
                ).join(
                    source_file_changes, source_file_changes.c.commit_id == commits.c.id
                )
            ).where(
                work_item_source_file_changes_temp.c.commit_id == None
            )
        )
    )

    upsert_stmt = insert(work_item_source_file_changes).from_select(
        [
            'commit_id',
            'work_item_id',
            'delivery_cycle_id',
            'repository_id',
            'source_file_id',
            'source_commit_id',
            'commit_date',
            'committer_contributor_alias_id',
            'author_contributor_alias_id',
            'num_parents',
            'created_on_branch',
            'file_action',
            'total_lines_changed',
            'total_lines_deleted',
            'total_lines_added'

        ],
        select([
            work_item_source_file_changes_temp.c.commit_id,
            work_item_source_file_changes_temp.c.work_item_id,
            work_item_source_file_changes_temp.c.delivery_cycle_id,
            work_item_source_file_changes_temp.c.repository_id,
            work_item_source_file_changes_temp.c.source_file_id.label('source_file_id'),
            work_item_source_file_changes_temp.c.source_commit_id,
            work_item_source_file_changes_temp.c.commit_date,
            work_item_source_file_changes_temp.c.committer_contributor_alias_id,
            work_item_source_file_changes_temp.c.author_contributor_alias_id,
            work_item_source_file_changes_temp.c.num_parents,
            work_item_source_file_changes_temp.c.created_on_branch,
            work_item_source_file_changes_temp.c.file_action,
            work_item_source_file_changes_temp.c.total_lines_changed,
            work_item_source_file_changes_temp.c.total_lines_deleted,
            work_item_source_file_changes_temp.c.total_lines_added
        ]
        ).select_from(
            work_item_source_file_changes_temp
        )
    )

    updated = session.connection().execute(
        upsert_stmt.on_conflict_do_update(
            index_elements=['commit_id', 'work_item_id', 'delivery_cycle_id', 'repository_id', 'source_file_id'],
            set_=dict(
                source_commit_id=upsert_stmt.excluded.source_commit_id,
                commit_date=upsert_stmt.excluded.commit_date,
                committer_contributor_alias_id=upsert_stmt.excluded.committer_contributor_alias_id,
                author_contributor_alias_id=upsert_stmt.excluded.author_contributor_alias_id,
                num_parents=upsert_stmt.excluded.num_parents,
                created_on_branch=upsert_stmt.excluded.created_on_branch,
                file_action=upsert_stmt.excluded.file_action,
                total_lines_changed=upsert_stmt.excluded.total_lines_changed,
                total_lines_deleted=upsert_stmt.excluded.total_lines_deleted,
                total_lines_added=upsert_stmt.excluded.total_lines_added
            )
        )
    ).rowcount
    return dict(
        updated=updated
    )


def populate_work_item_source_file_changes_for_commits(session, commit_details):
    updated = 0
    if len(commit_details) > 0:
        # create a temp table for received commit ids
        commits_temp = db.create_temp_table(
            table_name='commits_temp',
            columns=[
                Column('commit_key', UUID(as_uuid=True)),
            ]
        )
        commits_temp.create(session.connection(), checkfirst=True)

        # Get distinct commit keys from input
        distinct_commits = set()
        for entry in commit_details:
            distinct_commits.add(entry['key'])

        session.connection().execute(
            commits_temp.insert().values(
                [
                    dict(
                        commit_key=record
                    )
                    for record in distinct_commits
                ]
            )
        )
        result = populate_work_item_source_file_changes(session, commits_temp)
        updated = result['updated']
    return dict(
        updated=updated
    )


def populate_work_item_source_file_changes_for_work_items(session, work_items_commits):
    updated = 0
    if len(work_items_commits) > 0:
        # create a temp table for commit ids from work_items_commits
        commits_temp = db.create_temp_table(
            table_name='commits_temp',
            columns=[
                Column('commit_key', UUID(as_uuid=True)),
            ]
        )
        commits_temp.create(session.connection(), checkfirst=True)

        # Get distinct commit keys from input
        distinct_commits = set()
        for entry in work_items_commits:
            distinct_commits.add(entry['commit_key'])

        session.connection().execute(
            commits_temp.insert().values(
                [
                    dict(
                        commit_key=record
                    )
                    for record in distinct_commits
                ]
            )
        )
        result = populate_work_item_source_file_changes(session, commits_temp)
        updated = result['updated']

    return dict(
        updated=updated
    )


#####################################################################
# Methods required to update all delivery cycle dependent data
# whenever source state mapping is updated
#####################################################################

def delete_work_items_source_delivery_cycle_durations(session, work_items_source_id):
    session.connection().execute(
        work_item_delivery_cycle_durations.delete().where(
            work_item_delivery_cycle_durations.c.delivery_cycle_id.in_(select([
                work_item_delivery_cycles.c.delivery_cycle_id
            ]).where(
                and_(
                    work_items.c.id == work_item_delivery_cycles.c.work_item_id,
                    work_items.c.work_items_source_id == work_items_source_id
                )
            )
            )
        )
    )


def delete_work_items_source_delivery_cycle_contributors(session, work_items_source_id):
    session.connection().execute(
        work_item_delivery_cycle_contributors.delete().where(
            work_item_delivery_cycle_contributors.c.delivery_cycle_id.in_(select([
                work_item_delivery_cycles.c.delivery_cycle_id
            ]).where(
                and_(
                    work_items.c.id == work_item_delivery_cycles.c.work_item_id,
                    work_items.c.work_items_source_id == work_items_source_id
                )
            )
            )
        )
    )


def delete_work_items_source_source_file_changes(session, work_items_source_id):
    session.connection().execute(
        work_item_source_file_changes.delete().where(
            work_item_source_file_changes.c.work_item_id.in_(select([
                work_items.c.id
            ]).where(
                    work_items.c.work_items_source_id == work_items_source_id
                )
            )
        )
    )


def delete_work_items_source_delivery_cycles(session, work_items_source_id):
    session.connection().execute(
        work_item_delivery_cycles.delete().where(
            work_item_delivery_cycles.c.work_item_id.in_(select([
                work_items.c.id
            ]).where(
                work_items.c.work_items_source_id == work_items_source_id
            )
            )
        )
    )


def recompute_work_items_delivery_cycle_durations(session, work_items_source_id):
    # recompute and insert delivery cycle durations

    # calculate the start and end date of each state transition from the state transitions table.
    work_items_state_time_spans = select([
        work_item_delivery_cycles.c.delivery_cycle_id,
        work_item_delivery_cycles.c.work_item_id,
        work_item_state_transitions.c.state,
        work_item_state_transitions.c.created_at.label('start_time'),
        work_item_state_transitions.c.seq_no,
        (func.lead(work_item_state_transitions.c.created_at).over(
            partition_by=work_item_state_transitions.c.work_item_id,
            order_by=work_item_state_transitions.c.seq_no
        )).label('end_time')
    ]).select_from(
        work_item_delivery_cycles.join(
            work_item_state_transitions,
            work_item_delivery_cycles.c.work_item_id == work_item_state_transitions.c.work_item_id
        ).join(
            work_items, work_item_state_transitions.c.work_item_id == work_items.c.id
        )).where(
        work_items.c.work_items_source_id == work_items_source_id
    ).alias()

    # compute the duration in each state from the time_span in each state.
    work_items_duration_in_state = select([
        work_items_state_time_spans.c.delivery_cycle_id,
        work_items_state_time_spans.c.state,
        work_items_state_time_spans.c.start_time,
        work_items_state_time_spans.c.end_time,
        (extract('epoch', work_items_state_time_spans.c.end_time) -
         extract('epoch', work_items_state_time_spans.c.start_time)).label('duration')
    ]).select_from(
        work_items_state_time_spans
    ).alias()

    # aggregate the cumulative time in each state and insert into the delivery cycle durations table.
    session.connection().execute(
        work_item_delivery_cycle_durations.insert().from_select([
            'delivery_cycle_id',
            'state',
            'cumulative_time_in_state'
        ],

            select([
                (func.min(work_items_duration_in_state.c.delivery_cycle_id)).label('delivery_cycle_id'),
                work_items_duration_in_state.c.state.label('state'),
                (func.sum(work_items_duration_in_state.c.duration)).label('cumulative_time_in_state')
            ]).select_from(
                work_items_duration_in_state
            ).group_by(work_items_duration_in_state.c.delivery_cycle_id, work_items_duration_in_state.c.state)
        )
    )


def recompute_work_item_delivery_cycles_cycle_time(session, work_items_source_id):
    delivery_cycles_cycle_time = select([
        work_item_delivery_cycles.c.delivery_cycle_id.label('delivery_cycle_id'),
        work_item_delivery_cycles.c.work_item_id,
        func.sum(
            case(
                [
                    (
                        or_(
                            work_items_source_state_map.c.state_type == WorkItemsStateType.open.value,
                            work_items_source_state_map.c.state_type == WorkItemsStateType.wip.value,
                            work_items_source_state_map.c.state_type == WorkItemsStateType.complete.value
                        ),
                        work_item_delivery_cycle_durations.c.cumulative_time_in_state
                    )
                ],
                else_=None
            )).label('cycle_time')
    ]).select_from(
        work_items.join(
            work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
        ).join(
            work_item_delivery_cycle_durations,
            work_item_delivery_cycle_durations.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
        ).join(
            work_items_source_state_map,
            work_items_source_state_map.c.work_items_source_id == work_items.c.work_items_source_id
        )).where(
        and_(
            work_item_delivery_cycle_durations.c.state == work_items_source_state_map.c.state,
            work_item_delivery_cycles.c.end_date != None,
            work_items.c.work_items_source_id == work_items_source_id
        )
    ).group_by(
        work_item_delivery_cycles.c.delivery_cycle_id
    ).cte('delivery_cycles_cycle_time')

    updated = session.connection().execute(
        work_item_delivery_cycles.update().where(
            work_item_delivery_cycles.c.delivery_cycle_id == delivery_cycles_cycle_time.c.delivery_cycle_id
        ).values(
            cycle_time=delivery_cycles_cycle_time.c.cycle_time
        )
    ).rowcount


def recreate_work_items_source_delivery_cycles(session, work_items_source_id):
    # insert initial delivery cycles

    session.connection().execute(
        insert(work_item_delivery_cycles).from_select(
            [
                'work_item_id',
                'start_seq_no',
                'start_date'
            ],
            select([
                work_item_state_transitions.c.work_item_id,
                work_item_state_transitions.c.seq_no.label('start_seq_no'),
                work_item_state_transitions.c.created_at.label('start_date')
            ]).select_from(
                work_item_state_transitions.join(
                    work_items, work_items.c.id == work_item_state_transitions.c.work_item_id
                )
            ).where(
                and_(
                    work_item_state_transitions.c.seq_no == 0,
                    work_items.c.work_items_source_id == work_items_source_id
                )
            )
        )
    )

    # insert subsequent delivery cycles for reopened issues
    session.connection().execute(
        insert(work_item_delivery_cycles).from_select(
            [
                'work_item_id',
                'start_seq_no',
                'start_date'
            ],
            select([
                work_item_state_transitions.c.work_item_id,
                work_item_state_transitions.c.seq_no.label('start_seq_no'),
                work_item_state_transitions.c.created_at.label('start_date')
            ]).select_from(
                work_item_state_transitions.join(
                    work_items, work_items.c.id == work_item_state_transitions.c.work_item_id
                )
            ).where(
                and_(
                    work_item_state_transitions.c.previous_state == work_items_source_state_map.c.state,
                    work_items_source_state_map.c.state_type == WorkItemsStateType.closed.value,
                    work_items_source_state_map.c.work_items_source_id == work_items_source_id,
                    work_items.c.work_items_source_id == work_items_source_id
                )
            )
        )
    )

    # update delivery cycles for work_items transitioning to closed state_type
    # Update only for the first occurrence of transition to closed state
    earliest_closed_state_transition = select([
        work_item_delivery_cycles.c.work_item_id,
        work_item_delivery_cycles.c.delivery_cycle_id,
        func.min(work_item_state_transitions.c.seq_no).label('end_seq_no')
    ]).select_from(
        work_item_state_transitions.join(
            work_item_delivery_cycles, work_item_state_transitions.c.work_item_id == work_item_delivery_cycles.c.work_item_id
        ).join(
            work_items_source_state_map, work_item_state_transitions.c.state == work_items_source_state_map.c.state
        )).where(
            and_(
                work_items_source_state_map.c.state_type == WorkItemsStateType.closed.value,
                work_items_source_state_map.c.work_items_source_id == work_items_source_id,
                work_item_delivery_cycles.c.start_date <= work_item_state_transitions.c.created_at
            )
        ).group_by(
            work_item_delivery_cycles.c.delivery_cycle_id, work_item_delivery_cycles.c.work_item_id
        ).alias()

    session.connection().execute(
        work_item_delivery_cycles.update().where(
            and_(
                work_item_delivery_cycles.c.delivery_cycle_id == earliest_closed_state_transition.c.delivery_cycle_id,
                work_item_state_transitions.c.seq_no == earliest_closed_state_transition.c.end_seq_no,
                work_item_state_transitions.c.work_item_id == earliest_closed_state_transition.c.work_item_id
            )
        ).values(
            end_seq_no=earliest_closed_state_transition.c.end_seq_no,
            end_date=work_item_state_transitions.c.created_at,
            lead_time=(extract('epoch', work_item_state_transitions.c.created_at) - extract('epoch', work_item_delivery_cycles.c.start_date))
        )
    )

    # update current_delivery_cycle_id for work items
    latest_delivery_cycle = select([
        work_item_delivery_cycles.c.work_item_id.label('work_item_id'),
        (func.max(work_item_delivery_cycles.c.delivery_cycle_id)).label('delivery_cycle_id'),
    ]).select_from(
        work_item_delivery_cycles.join(
            work_items, work_items.c.id == work_item_delivery_cycles.c.work_item_id
        )).where(
        work_items.c.work_items_source_id == work_items_source_id
    ).group_by(
        work_item_delivery_cycles.c.work_item_id
    ).alias()

    updated = session.connection().execute(
        work_items.update().where(
            work_items.c.work_items_source_id == work_items_source_id
        ).values(
            current_delivery_cycle_id=select([
                latest_delivery_cycle.c.delivery_cycle_id.label('current_delivery_cycle_id')
            ]).where(
                latest_delivery_cycle.c.work_item_id == work_items.c.id
            )
        )
    )
    return updated


def update_work_items_source_delivery_cycles(session, work_items_source_id):
    # set current_delivery_cycle_id to none for work items in given source
    session.connection().execute(
        work_items.update().values(
            current_delivery_cycle_id=None
        ).where(
            work_items.c.work_items_source_id == work_items_source_id
        )
    )

    # create a temp table for work item keys of given source
    work_items_temp = db.create_temp_table(
        table_name='work_items_temp',
        columns=[
            Column('work_item_key', UUID(as_uuid=True)),
            Column('work_item_id', Integer)
        ]
    )
    work_items_temp.create(session.connection(), checkfirst=True)
    # populate the temp table
    session.connection().execute(
        work_items_temp.insert().from_select(
            [
                'work_item_key',
                'work_item_id'
            ],
            select([
                work_items.c.key,
                work_items.c.id
            ]).select_from(
                work_items
            ).where(
                work_items.c.work_items_source_id == work_items_source_id
            )
        )
    )

    # create a temp table for commits from the given source
    commits_temp = db.create_temp_table(
        table_name='commits_temp',
        columns=[
            Column('commit_key', UUID(as_uuid=True)),
        ]
    )

    commits_temp.create(session.connection(), checkfirst=True)
    # populate the temp table
    session.connection().execute(
        commits_temp.insert().from_select(
            [
                'commit_key',
            ],
            select([
                distinct(commits.c.key)
            ]).select_from(
                work_items.join(
                    work_items_commits_table, work_items_commits_table.c.work_item_id == work_items.c.id
                ).join(
                    commits, commits.c.id == work_items_commits_table.c.commit_id
                )
            ).where(
                work_items.c.work_items_source_id == work_items_source_id
            )
        )
    )

    # delete work_item_source_file_changes for work items for given work items source
    delete_work_items_source_source_file_changes(session, work_items_source_id)

    # delete all work_item_delivery_cycle_contributors for given work items source
    delete_work_items_source_delivery_cycle_contributors(session, work_items_source_id)

    # delete all delivery cycle durations for given work items source
    delete_work_items_source_delivery_cycle_durations(session, work_items_source_id)

    # delete all delivery cycles for given work items source
    delete_work_items_source_delivery_cycles(session, work_items_source_id)

    # Recreate delivery cycles for work items for given work items source
    updated = recreate_work_items_source_delivery_cycles(session, work_items_source_id)

    # Recompute and insert the deleted delivery cycle durations, based on new delivery cycles
    recompute_work_items_delivery_cycle_durations(session, work_items_source_id)

    # Recompute other delivery cycle fields
    compute_work_item_delivery_cycle_commit_stats(session, work_items_temp)
    compute_implementation_complexity_metrics(session, work_items_temp)

    # Repopulate work_item_delivery_cycle_contributors
    compute_contributor_metrics(session, work_items_temp)

    # Repopulate work_item_source_file_changes
    populate_work_item_source_file_changes(session, commits_temp)

    return updated

