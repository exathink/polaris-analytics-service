# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response, abort
from flask_cors import cross_origin
from flask_login import current_user

from .access_control import has_org_access

from polaris.analytics.datasources.projects import \
    ActivitySummary, \
    ActivitySummaryByRepository


projects_api = Blueprint('projects_api', __name__)

@projects_api.route('/activity-summary/<organization_name>/<project_name>')
@cross_origin(supports_credentials=True)
def activity_summary(organization_name, project_name):
    return has_org_access(current_user, organization_name) and \
           make_response(ActivitySummary().for_project(organization_name, project_name)), \
           {'Content-Type': 'application/json'}



@projects_api.route('/activity-summary-by-repository/<organization_name>/<project_name>')
@cross_origin(supports_credentials=True)
def activity_summary_by_project(organization_name, project_name):
    return has_org_access(current_user, organization_name) and \
           make_response(ActivitySummaryByRepository().for_project(organization_name, project_name)), \
           {'Content-Type': 'application/json'}



