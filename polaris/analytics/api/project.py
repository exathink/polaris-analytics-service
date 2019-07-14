# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from polaris.analytics.db.model import Project
from polaris.utils.exceptions import ProcessingException


def archive_project(project_key, join_this=None):
    with db.orm_session(join_this) as session:
        project = Project.find_by_project_key(session, project_key)
        if project is not None:
            project.archived = True
            return project.name
        else:
            raise ProcessingException(f'Could not find project with key: {project_key}')