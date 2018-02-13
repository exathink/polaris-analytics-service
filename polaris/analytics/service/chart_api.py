# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response
from flask_cors import cross_origin

from polaris.analytics.db import charts


chart_api = Blueprint('chart_api', __name__)

@chart_api.route('/')
def index():
    return 'ping'


@chart_api.route('/project-summary/<organization_name>/')
def project_summary(organization_name):
    model = charts.ProjectLandscapeChartModel(many=True)

    return make_response(model.dumps(model.get(organization_name))),  \
        {'Content-Type': 'application/json'}


