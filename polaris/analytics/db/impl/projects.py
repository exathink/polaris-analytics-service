# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from sqlalchemy.sql.expression import and_

from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.db.model import Project, WorkItemsSource, work_items, work_items_source_state_map, WorkItem, work_item_delivery_cycles
from polaris.utils.collections import find
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.analytics.db.impl')


def update_work_items_computed_state_types(session, work_items_source_id, update_delivery_cycle):
    logger.info('--update_delivery_cycle-------- ' + str(update_delivery_cycle))
    updated = session.execute(
        work_items.update().values(
            state_type=None
        ).where(
            work_items.c.work_items_source_id == work_items_source_id
        )
    )
    if not update_delivery_cycle:
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
    else:
        session.execute(
            work_items.update().values(
                state_type=work_items_source_state_map.c.state_type,
                current_delivery_cycle_id=None
            ).where(
                and_(
                    work_items.c.state == work_items_source_state_map.c.state,
                    work_items.c.work_items_source_id == work_items_source_id
                )
            )
        )

        work_items_list = WorkItem.find_by_work_items_source_key(session, work_items_source_id)
        for work_item in work_items_list:
            session.execute(
                work_item_delivery_cycles.delete().where(
                    work_item_delivery_cycles.work_item_id == work_item.id
                )
            )
    return updated


def update_work_items_source_state_mapping(session, work_items_source_key, state_mappings, update_delivery_cycle):
    logger.info('*************')
    work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
    if work_items_source is not None:
        work_items_source.init_state_map(state_mappings)
        session.flush()
        logger.info('*************')
        update_work_items_computed_state_types(session, work_items_source.id, update_delivery_cycle)

def update_project_work_items_source_state_mappings(session, project_state_maps):
    logger.info("Inside update_project_work_items_state_mappings")
    updated = []
    # Check if project exists. Not sure if this is required
    project = Project.find_by_project_key(session, project_state_maps.project_key)
    if project is not None:
        # Find and update corresponding work items source state maps
        for work_items_source_map in project_state_maps.work_items_source_state_maps:
            source_key = work_items_source_map.work_items_source_key
            closed = [state_map for state_map in work_items_source_map.state_maps if
                      state_map.state_type == WorkItemsStateType.closed.value]
            logger.info('------------------')
            logger.info('-----new_closed_state----' + closed)
            if len(closed) > 1:
                raise ProcessingException(f'Work Items Source can have only one closed state')
            else:
                work_item_source = find(project.work_items_sources,
                                         lambda work_item_source: str(work_item_source.key) == str(source_key))
                if work_item_source:
                    old_closed_state = find(work_item_source.state_maps,
                                            lambda w: str(w.state_type) == str(WorkItemsStateType.closed.value))
                    logger.info('-----old_closed_state----' + str(old_closed_state.state))
                    logger.info('-----new_closed_state----' + str(closed.state))
                    if old_closed_state.state != closed.state:
                        update_work_items_source_state_mapping(session, source_key, work_items_source_map.state_maps, True)
                    else:
                        update_work_items_source_state_mapping(session, source_key, work_items_source_map.state_maps,
                                                               False)
                    updated.append(source_key)

                else:
                    raise ProcessingException(f'Work Items Source with key {source_key} does not belong to project')

    else:
        raise ProcessingException(f'Could not find project with key {project_state_maps.project_key}')

    return dict(
        project_key=project_state_maps.project_key,
        work_items_sources=updated
    )
