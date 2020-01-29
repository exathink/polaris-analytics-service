# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from polaris.common import db
from polaris.analytics.db.model import Project, WorkItemsSource
from polaris.utils.exceptions import ProcessingException
from polaris.utils.collections import find

logger = logging.getLogger('polaris.analytics.api.project')


def archive_project(project_key, join_this=None):
    with db.orm_session(join_this) as session:
        project = Project.find_by_project_key(session, project_key)
        if project is not None:
            project.archived = True
            return project.name
        else:
            raise ProcessingException(f'Could not find project with key: {project_key}')


def update_project_state_maps(project_state_maps, join_this=None):
    logger.info("Inside update_project_state_maps")
    with db.orm_session(join_this) as session:

        # Check if project exists. Not sure if this is required
        project_key = project_state_maps.project_key
        project = Project.find_by_project_key(session, project_key)
        if project is None:
            raise ProcessingException(f'Could not find project with key: {project_key}')

        # Find and update corresponding work items source state maps
        for work_items_source_state_map in project_state_maps.work_items_source_state_maps:
            source_key = work_items_source_state_map.work_items_source_key
            work_items_source = find(project.work_items_sources, lambda work_item_source: str(work_item_source.key) == str(source_key))
            logger.info(work_items_source)
            if work_items_source is not None:
                work_items_source.init_state_map(work_items_source_state_map.state_maps)
            else:
                raise ProcessingException(f'Work item source with key: {source_key} is not associated to project with key: {project_key}')
    return True