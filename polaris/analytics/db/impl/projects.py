# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging


from sqlalchemy.sql.expression import and_

from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.db.model import Project, WorkItemsSource, work_items, work_items_source_state_map
from polaris.utils.collections import find
from polaris.utils.exceptions import ProcessingException

from .delivery_cycle_tracking import update_work_items_source_delivery_cycles, \
    recompute_work_item_delivery_cycles_cycle_time

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


def update_work_items_source_state_mapping(session, work_items_source_key, state_mappings):
    work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
    if work_items_source is not None:
        old_closed_states = set()
        for w in work_items_source.state_maps:
            if w.state_type == str(WorkItemsStateType.closed.value):
                old_closed_states.add(w.state)
        new_closed_states = set()
        for w in state_mappings:
            if w.state_type == str(WorkItemsStateType.closed.value):
                new_closed_states.add(w.state)
        work_items_source.init_state_map(state_mappings)
        session.flush()

        # update state type in work items based on new mapping
        update_work_items_computed_state_types(session, work_items_source.id)

        # If old closed state is not same as new closed state
        # FIXME: To handle multiple closed states
        if (not old_closed_states and new_closed_states) \
                or (old_closed_states and not new_closed_states) \
                or (new_closed_states and old_closed_states \
                    and old_closed_states != new_closed_states):
            update_work_items_source_delivery_cycles(session, work_items_source.id)

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
