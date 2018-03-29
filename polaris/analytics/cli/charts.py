# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import argh
import logging
import os

import polaris.analytics.datasources.organizations.project_activity_summary
from polaris.common import db
from polaris.analytics.db import charts
from polaris.utils.logging import config_logging
from polaris.utils.timer import Timer


@argh.arg('organization-name', nargs='*')
@argh.arg('--outfile', default='project_landscape')
def project_landscape(organization_name, outfile=None):
    with Timer('Operation', loglevel=logging.INFO):
        org_name = ' '.join(organization_name)
        model = polaris.analytics.datasources.organizations.project_activity_summary.ProjectLandscapeChartModel(many=True)
        with open(outfile, mode='w') as file:
            file.write(model.dumps(model.get(org_name)).data)





if __name__ == '__main__':
    db.init()
    config_logging()
    argh.dispatch_commands([project_landscape])


