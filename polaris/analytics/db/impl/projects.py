# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from sqlalchemy.sql.expression import and_, select, distinct, bindparam, update

from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.db.model import Project, WorkItemsSource, work_items, work_items_source_state_map, \
    work_item_state_transitions, projects_repositories
from polaris.utils.collections import find
from polaris.utils.exceptions import ProcessingException

from .delivery_cycle_tracking import update_work_items_source_delivery_cycles, \
    recompute_work_item_delivery_cycles_cycle_time

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
                work_items.c.work_items_source_id == work_items_source_id,
                work_items_source_state_map.c.work_items_source_id == work_items_source_id,
            )
        )
    )
    return updated


def get_existing_work_item_states_from_transitions(session, work_items_source):
    # Checking for any existing states in state transitions table, which are newly mapped or unmapped
    # Getting all distinct states in work item state transitions for work items in work item source
    existing_work_item_states = set([
        state[0]
        for state in session.connection().execute(
            select([
                distinct(work_item_state_transitions.c.state)
            ]).select_from(
                work_item_state_transitions.join(
                    work_items, work_items.c.id == work_item_state_transitions.c.work_item_id
                )
            ).where(
                work_items.c.work_items_source_id == work_items_source.id
            )
        ).fetchall()
    ])
    return existing_work_item_states


def update_work_items_source_state_mapping(session, work_items_source_key, state_mappings):
    work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
    if work_items_source is not None:
        all_work_item_states = get_existing_work_item_states_from_transitions(session, work_items_source)
        current_states_with_mapping = {source_state_map.state for source_state_map in work_items_source.state_maps}

        current_unmapped_states = {}
        if len(all_work_item_states) > 0:
            current_unmapped_states = all_work_item_states - current_states_with_mapping

        current_closed_states = {
            source_state_map.state
            for source_state_map in work_items_source.state_maps
            if source_state_map.state_type == WorkItemsStateType.closed.value
        }

        new_closed_states = {
            source_state_map.state
            for source_state_map in state_mappings
            if source_state_map.state_type == WorkItemsStateType.closed.value
        }

        # Initialize the new state map
        work_items_source.init_state_map(state_mappings)
        session.flush()

        # update state type in work items based on new mapping
        update_work_items_computed_state_types(session, work_items_source.id)

        # If old closed state is not same as new closed state, or there were any unmapped states
        # before the new mapping was initialized, we need to recreate the delivery cycles.

        if current_closed_states != new_closed_states or len(current_unmapped_states) > 0:
            update_work_items_source_delivery_cycles(session, work_items_source.id)

        # Recompute cycle time as it is dependent on state type mapping
        # Directly impacted if mapping change includes state types: open, wip, complete
        # Also needs to be recomputed is closed state type changes as delivery cycles are recreated then
        # So need to recompute for all cases except when only state mapping is changed for 'backlog'
        # That may be once in a while, so updating every time state map changes
        recompute_work_item_delivery_cycles_cycle_time(session, work_items_source.id)


def update_project_work_items(session, project_work_items):
    updated = []
    project = Project.find_by_project_key(session, project_work_items.project_key)
    if project is not None:
        stmt = work_items.update(). \
            where(work_items.c.key == bindparam('work_item_key')). \
            values({
            'budget': bindparam('budget')
        })
        rowcount = session.connection().execute(stmt, project_work_items.work_items_info).rowcount
        if rowcount != len(project_work_items.work_items_info):
            raise ProcessingException(f"Could not update project work items")
    else:
        raise ProcessingException(f'Could not find project with key {project_work_items.project_key}')

    return dict(
        project_key=project_work_items.project_key,
        work_items_keys=[info['work_item_key'] for info in project_work_items.work_items_info]
    )


def update_project_work_items_source_state_mappings(session, project_state_maps):
    updated = []
    # Check if project exists
    project = Project.find_by_project_key(session, project_state_maps.project_key)
    if project is not None:
        # Find and update corresponding work items source state maps
        for work_items_source_state_mapping in project_state_maps.work_items_source_state_maps:
            source_key = work_items_source_state_mapping.work_items_source_key
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


def update_project_settings(session, update_project_settings_input):
    project = Project.find_by_project_key(session, update_project_settings_input.key)
    if project is not None:
        project.update_settings(update_project_settings_input)
        return dict(
            key=project.key
        )
    else:
        raise ProcessingException(f'Could not find project with key: {update_project_settings_input.key}')


def update_project_excluded_repositories(session, update_project_excluded_repositories_input):
    project = Project.find_by_project_key(session, update_project_excluded_repositories_input.project_key)
    if project is not None:
        for exclusion in update_project_excluded_repositories_input.exclusions:
            repository_rel = find(project.repositories_rel, lambda rel: str(rel.repository.key) == exclusion.repository_key)
            if repository_rel is not None:
                repository_rel.excluded = exclusion.excluded
            else:
                raise ProcessingException(f"Could not find repository with key {exclusion.repository_key} in this project")

        return dict(
            key=project.key
        )
    else:
        raise ProcessingException(f'Could not find project with key: {update_project_excluded_repositories_input.project_key}')
