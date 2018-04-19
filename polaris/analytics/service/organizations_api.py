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

from polaris.analytics.datasources.organizations import \
    ActivitySummary, \
    ActivitySummaryByProject


organizations_api = Blueprint('organizations_api', __name__)

@organizations_api.route('/activity-summary/<organization_name>/')
@cross_origin(supports_credentials=True)
def activity_summary(organization_name):
    return has_org_access(current_user, organization_name) and \
           make_response(ActivitySummary().for_organization(organization_name)), \
           {'Content-Type': 'application/json'}



@organizations_api.route('/activity-summary-by-project/<organization_name>/')
@cross_origin(supports_credentials=True)
def activity_summary_by_project(organization_name):
    return has_org_access(current_user, organization_name) and \
           make_response(ActivitySummaryByProject().for_organization(organization_name)), \
           {'Content-Type': 'application/json'}



