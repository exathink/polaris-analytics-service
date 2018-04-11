# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response
from flask_cors import cross_origin
from flask_login import current_user

from polaris.analytics.datasources.organizations import ProjectActivitySummary
from polaris.analytics.datasources.accounts import OrganizationActivitySummary

viz_api = Blueprint('chart_api', __name__)


@viz_api.route('/')
@cross_origin()
def index():
    return 'ping'


@viz_api.route('/account-activity-summary/')
@cross_origin(supports_credentials=True)
def account_activity_summary():
    user_info = current_user.user_config
    if user_info:
        activity_summary = OrganizationActivitySummary()
        if 'admin' not in current_user.roles:
            response = activity_summary.for_all_orgs()
        else:
            response = activity_summary.for_account(user_info['account']['account_key'])
        return make_response(response), \
               {'Content-Type': 'application/json'}


@viz_api.route('/project-summary/<organization_name>/')
@cross_origin(supports_credentials=True)
def project_summary(organization_name):
    activity_summary = ProjectActivitySummary()

    return make_response(activity_summary.for_organization(organization_name)), \
           {'Content-Type': 'application/json'}
