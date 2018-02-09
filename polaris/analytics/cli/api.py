# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import yaml
import logging
from polaris.repos.db.api import projects as projects_api
from polaris.common import db

logger = logging.getLogger('polaris.analytics.cli.api')

def yaml_load(load_file):
    with open(load_file) as file:
        return yaml.load(file)

def yaml_dump(dump_file, document):
    try:
        open(dump_file, 'x').close()
    except OSError:
        pass

    with open(dump_file, 'w') as file:
        return yaml.dump(document, file, default_flow_style=False)

def load_project(organization_name, project):
    return projects_api.add_or_update_project(organization_name, project['name'], project['properties'], repositories=project.get('repositories'))


def load_projects(organization_name, projects):
    added = 0
    updated = 0
    with db.orm_session() as session:
        for project in projects:
            logger.info("loading project {}".format(project['name']))
            if projects_api.add_or_update_project(organization_name, project['name'], project['properties'], repositories=project.get('repositories'), session=session):
                added = added + 1
            else:
                updated = updated + 1

    return added, updated

