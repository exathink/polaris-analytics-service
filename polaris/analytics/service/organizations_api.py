# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response
from flask_cors import cross_origin
from flask_login import current_user

from polaris.analytics.datasources.organizations import OrganizationActivitySummaryByProject
from polaris.analytics.datasources.accounts import AccountActivitySummary

viz_api = Blueprint('viz_api', __name__)


@viz_api.route('/')
@cross_origin()
def index():
    return 'ping'


@viz_api.route('/account-organizations-activity-summary/')
@cross_origin(supports_credentials=True)
def account_organizations_activity_summary():
    user_info = current_user.user_config
    if user_info:
        activity_summary = AccountActivitySummary()
        if 'admin' in current_user.roles:
            response = activity_summary.for_all_orgs()
        else:
            response = activity_summary.for_account(user_info['account']['account_key'])
        return make_response(response), \
               {'Content-Type': 'application/json'}


@viz_api.route('/organization-projects-activity-summary/<organization_name>/')
@cross_origin(supports_credentials=True)
def organization_projects_activity_summary(organization_name):
    activity_summary = OrganizationActivitySummaryByProject()

    return make_response(activity_summary.for_organization(organization_name)), \
           {'Content-Type': 'application/json'}
