# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



import yaml
import argh
import logging

from polaris.utils.logging import config_logging
from polaris.utils.collections import dict_merge, find

from polaris.common import db
from polaris.analytics.cli import api

logger = logging.getLogger('polaris.cli.analytics.api.main')




def load_project_file(project_file, project_name=None):
    document = api.yaml_load(project_file)
    organization_name = document['organization_name']
    if project_name:
        project = find(document['projects'], lambda proj: proj['name'] == project_name)
        if project:
            logger.info(project)
            api.load_project(organization_name, project)
        else:
            logger.error("Project {} was not found".format(project))

    else:
        for project in document['projects']:
            api.load_project(organization_name, project)


if __name__ == '__main__':
    config_logging(suppress=['requests.packages.urllib3.connectionpool'])
    db.init()
    argh.dispatch_commands([
        load_project_file
    ])
