# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response
from flask_cors import cross_origin

from polaris.analytics.datasources.organizations import ProjectActivitySummary



viz_api = Blueprint('chart_api', __name__)

@viz_api.route('/')
@cross_origin()
def index():
    return 'ping'


@viz_api.route('/project-summary/<organization_name>/')
@cross_origin(supports_credentials=True)
def project_summary(organization_name):
    activity_summary = ProjectActivitySummary()

    return make_response(activity_summary.for_organization(organization_name)),  \
        {'Content-Type': 'application/json'}


