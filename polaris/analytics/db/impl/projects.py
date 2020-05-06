# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from sqlalchemy import func, cast, Integer, distinct
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.expression import and_, select, extract, or_, case

from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.db.model import Project, WorkItemsSource, work_items, work_items_source_state_map, \
    work_item_delivery_cycles, work_item_state_transitions, work_item_delivery_cycle_durations, \
    work_item_delivery_cycle_contributors, contributor_aliases, commits, work_items_commits as work_items_commits_table
from polaris.utils.collections import find
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.analytics.db.impl')


def update_work_items_computed_state_types(session, work_items_source_id):
    logger.debug("Inside update work items computed state types")
    updated = session.execute(
        work_items.update().values(
            state_type=None
        ).where(
            work_items.c.work_items_source_id == work_items_source_id
        )
    )
    session.execute(
        work_items.update().values(
            state_type=work_items_source_state_map.c.state_type
        ).where(
            and_(
                work_items.c.state == work_items_source_state_map.c.state,
                work_items.c.work_items_source_id == work_items_source_id
            )
        )
    )
    return updated


def delete_work_item_delivery_cycle_durations(session, work_items_source_id):
    session.execute(
        work_item_delivery_cycle_durations.delete().where(
            work_item_delivery_cycle_durations.c.delivery_cycle_id.in_(select([
                work_item_delivery_cycles.c.delivery_cycle_id
            ]).where(
                and_(
                    work_item_delivery_cycle_durations.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id,
                    work_items.c.id == work_item_delivery_cycles.c.work_item_id,
                    work_items.c.work_items_source_id == work_items_source_id
                )
            )
            )
        )
    )


def delete_work_item_delivery_cycle_contributors(session, work_items_source_id):
    session.execute(
        work_item_delivery_cycle_contributors.delete().where(
            work_item_delivery_cycle_contributors.c.delivery_cycle_id.in_(select([
                work_item_delivery_cycles.c.delivery_cycle_id
            ]).where(
                and_(
                    work_item_delivery_cycle_durations.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id,
                    work_items.c.id == work_item_delivery_cycles.c.work_item_id,
                    work_items.c.work_items_source_id == work_items_source_id
                )
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


def repopulate_work_item_delivery_cycle_contributors(session, work_items_source_id):
    # The following metrics are calculated and updated for each work_item_delivery_cycle_contributor
    # 1. total_lines_as_author
    # 2. total_lines_as_reviewer

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
        work_item_delivery_cycles.join(
            work_items, work_item_delivery_cycles.c.work_item_id == work_items.c.id
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
            ),
            work_items.c.work_items_source_id == work_items_source_id
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


def recompute_work_item_delivery_cycles_commit_stats(session, work_items_source_id):
    # The following commit stats are calculated and updated for each work_item_delivery_cycle
    # 1. earliest_commit, latest_commit: earliest and latest commit for a work item delivery cycle
    # 2. repository_count: distinct repository count over all commits during a delivery cycle for a work item
    # 3. commit_count: distinct commit count over all commits during a delivery cycle for a work item

    updated = 0

    # select relevant rows to find various metrics
    delivery_cycles_commits_rows = select([
        work_item_delivery_cycles.c.delivery_cycle_id.label('delivery_cycle_id'),
        func.min(commits.c.commit_date).label('earliest_commit'),
        func.max(commits.c.commit_date).label('latest_commit'),
        func.count(distinct(commits.c.id)).label('commit_count'),
        func.count(distinct(commits.c.repository_id)).label('repository_count')
    ]).select_from(
        work_item_delivery_cycles.join(
            work_items, work_item_delivery_cycles.c.work_item_id == work_items.c.id
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
            ),
            work_items.c.work_items_source_id == work_items_source_id
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


def recompute_work_item_delivery_cycles_complexity_metrics(session, work_items_source_id):
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
        work_item_delivery_cycles.join(
            work_items, work_item_delivery_cycles.c.work_item_id == work_items.c.id
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
            ),
            work_items.c.work_items_source_id == work_items_source_id
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


def update_work_items_delivery_cycles(session, work_items_source_id):
    # set current_delivery_cycle_id to none for work items in given source
    session.execute(
        work_items.update().values(
            current_delivery_cycle_id=None
        ).where(
            work_items.c.work_items_source_id == work_items_source_id
        )
    )

    # delete all work_item_delivery_cycle_contributors for given work items source
    delete_work_item_delivery_cycle_contributors(session, work_items_source_id)

    # delete all delivery cycle durations for given work items source
    delete_work_item_delivery_cycle_durations(session, work_items_source_id)

    # delete all delivery cycles for given work items source
    session.execute(
        work_item_delivery_cycles.delete().where(
            work_item_delivery_cycles.c.work_item_id.in_(select([
                work_items.c.id
            ]).where(
                work_items.c.work_items_source_id == work_items_source_id
            )
            )
        )
    )

    # insert initial delivery cycles
    session.execute(
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
    session.execute(
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
    session.execute(
        work_item_delivery_cycles.update().values(
            end_seq_no=work_item_state_transitions.c.seq_no,
            end_date=work_item_state_transitions.c.created_at,
            lead_time=extract('epoch', work_item_state_transitions.c.created_at) - \
                      extract('epoch', work_item_delivery_cycles.c.start_date)
        ).where(
            and_(
                work_item_delivery_cycles.c.work_item_id == work_item_state_transitions.c.work_item_id,
                work_item_state_transitions.c.state == work_items_source_state_map.c.state,
                work_items_source_state_map.c.state_type == WorkItemsStateType.closed.value,
                work_items_source_state_map.c.work_items_source_id == work_items_source_id,
                work_item_delivery_cycles.c.start_date < work_item_state_transitions.c.created_at
            )
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

    updated = session.execute(
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

    # Recompute and insert the deleted delivery cycle durations, based on new delivery cycles
    recompute_work_items_delivery_cycle_durations(session, work_items_source_id)
    # Recompute other delivery cycle fields
    recompute_work_item_delivery_cycles_commit_stats(session, work_items_source_id)
    recompute_work_item_delivery_cycles_complexity_metrics(session, work_items_source_id)

    # Repopulate work_item_delivery_cycle_contributors
    repopulate_work_item_delivery_cycle_contributors(session, work_items_source_id)

    return updated


def update_work_items_source_state_mapping(session, work_items_source_key, state_mappings):
    work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
    if work_items_source is not None:
        old_closed_state = find(work_items_source.state_maps,
                                lambda w: str(w.state_type) == str(WorkItemsStateType.closed.value))
        new_closed_state = find(state_mappings,
                                lambda w: str(w.state_type) == str(WorkItemsStateType.closed.value))
        work_items_source.init_state_map(state_mappings)
        session.flush()

        # update state type in work items based on new mapping
        update_work_items_computed_state_types(session, work_items_source.id)

        # If old closed state is not same as new closed state
        if (old_closed_state is None and new_closed_state is not None) \
                or (old_closed_state is not None and new_closed_state is None) \
                or (new_closed_state is not None and old_closed_state is not None \
                    and old_closed_state.state != new_closed_state.state):
            update_work_items_delivery_cycles(session, work_items_source.id)

        # Recompute cycle time as it is dependent on state type mapping
        # Directly impacted if mapping change includes state types: open, wip, complete
        # Also needs to be recomputed is closed state type changes as delivery cycles are recreated then
        # So need to recompute for all cases except when only state mapping is changed for 'backlog'
        # That may be once in a while, so updating every time state map changes
        recompute_work_item_delivery_cycles_cycle_time(session, work_items_source.id)


def update_project_work_items_source_state_mappings(session, project_state_maps):
    updated = []
    # Check if project exists
    project = Project.find_by_project_key(session, project_state_maps.project_key)
    if project is not None:
        # Find and update corresponding work items source state maps
        for work_items_source_state_mapping in project_state_maps.work_items_source_state_maps:
            source_key = work_items_source_state_mapping.work_items_source_key
            closed = [state_map for state_map in work_items_source_state_mapping.state_maps if
                      state_map.state_type == WorkItemsStateType.closed.value]
            if len(closed) > 1:
                raise ProcessingException(f'Work Items Source can have only one closed state')
            else:
                work_items_source = find(project.work_items_sources,
                                         lambda work_item_source: str(work_item_source.key) == str(source_key))
                if work_items_source is not None:
                    update_work_items_source_state_mapping(session, source_key,
                                                           work_items_source_state_mapping.state_maps)
                    updated.append(source_key)
                else:
                    raise ProcessingException(f'Work Items Source with key {source_key} does not belong to project')
    else:
        raise ProcessingException(f'Could not find project with key {project_state_maps.project_key}')

    return dict(
        project_key=project_state_maps.project_key,
        work_items_sources=updated
    )
