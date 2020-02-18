# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from polaris.common import db
from polaris.analytics.db.model import Project, WorkItemsSource, work_items
from polaris.utils.exceptions import ProcessingException
from polaris.utils.collections import find

from sqlalchemy.sql.expression import bindparam

logger = logging.getLogger('polaris.analytics.db.impl')


def sync_work_items_state_mappings(session, work_items_source):
    state_types = [dict(
            state_type=work_items_source.get_state_type(work_item.state),
            _id=work_item.id
        )
        for work_item in work_items_source.work_items]
    try:
        updated = session.execute(
            work_items.update().where(
                work_items.c.id == bindparam('_id')
            ).values({
                'state_type': bindparam('state_type')
          }),
            state_types).rowcount
    except Exception as e:
        return ProcessingException(\
            f'Could not sync work items for work_items_source with key: \
            {work_items_source.key}', e)
    return updated


def update_project_work_items_source_state_mappings(session, project_state_maps):
    logger.info("Inside update_project_work_items_state_mappings")

    # Check if project exists. Not sure if this is required
    project_key = project_state_maps.project_key
    project = Project.find_by_project_key(session, project_key)
    if project is None:
        raise ProcessingException(f'Could not find project with key: {project_key}')

    # Find and update corresponding work items source state maps
    for work_items_source_state_map in project_state_maps.work_items_source_state_maps:
        source_key = work_items_source_state_map.work_items_source_key
        work_items_source = find(project.work_items_sources,
                                 lambda work_item_source: str(work_item_source.key) == str(source_key))
        if work_items_source is not None:
            work_items_source.init_state_map(work_items_source_state_map.state_maps)
            logger.info("Updated work items with state_map")
        else:
            raise ProcessingException(f'Work item source with key: {source_key} '
                                          f'is not associated to project with key: {project_key}')
    return work_items_source

