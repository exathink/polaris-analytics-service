# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from polaris.utils.exceptions import ProcessingException

from polaris.analytics.db.model import ValueStream, Project

def create_value_stream(session, project_key, name, description, work_item_selectors):
    project = Project.find_by_project_key(session, project_key)
    if project is not None:
        value_stream_key=uuid.uuid4()
        project.value_streams.append(
            ValueStream(
                key=value_stream_key,
                name=name,
                description=description,
                work_item_selectors=work_item_selectors
            )
        )
        return dict(
            key=value_stream_key
        )
    else:
        raise ProcessingException(f"Could not find project with key {project_key}")