# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.expression import and_, select, extract

from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.db.model import Project, WorkItemsSource, work_items, work_items_source_state_map, \
    work_item_delivery_cycles, work_item_state_transitions, work_item_delivery_cycle_durations
from polaris.utils.collections import find
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.analytics.db.impl')


def update_work_items_computed_state_types(session, work_items_source_id):
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


def update_work_items_delivery_cycles(session, work_items_source_id):
    logger.info("Update work item delivery cycles")
    # set current_delivery_cycle_id to none for work items in given source
    session.execute(
        work_items.update().values(
            current_delivery_cycle_id=None
        ).where(
            work_items.c.work_items_source_id == work_items_source_id
        )
    )

    # delete all delivery cycle durations for given work items source
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
    updated = session.execute(
        work_items.update().values(
            current_delivery_cycle_id=work_item_delivery_cycles.c.delivery_cycle_id
        ).where(
            and_(
                work_items.c.state == work_items_source_state_map.c.state,
                work_items.c.work_items_source_id == work_items_source_id,
                work_item_delivery_cycles.c.work_item_id == work_items.c.id,
                work_item_delivery_cycles.c.delivery_cycle_id > work_items.c.current_delivery_cycle_id
            )
        )
    )

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
        update_work_items_computed_state_types(session, work_items_source.id)

        # If old closed state is not same as new closed state
        logger.info(f"old_closed_state {old_closed_state}, new_closed_state {new_closed_state}")
        if new_closed_state is not None:
            logger.info(f"{new_closed_state.state}, {new_closed_state.state_type}")
            if old_closed_state is None or old_closed_state.state != new_closed_state.state:
                update_work_items_delivery_cycles(session, work_items_source.id)


def update_project_work_items_source_state_mappings(session, project_state_maps):
    logger.info("Inside update_project_work_items_state_mappings")
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
                    update_work_items_source_state_mapping(session, source_key, work_items_source_state_mapping.state_maps)
                    updated.append(source_key)
                else:
                    raise ProcessingException(f'Work Items Source with key {source_key} does not belong to project')
    else:
        raise ProcessingException(f'Could not find project with key {project_state_maps.project_key}')

    return dict(
        project_key=project_state_maps.project_key,
        work_items_sources=updated
    )
