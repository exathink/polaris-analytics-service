# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response
from flask_cors import cross_origin
from flask_login import current_user
from polaris.analytics.datasources.activities.activity_summary import ActivitySummary

from .access_control import has_org_access

activity_summary_api = Blueprint('activity_summary_api', __name__)


@activity_summary_api.route('/account/')
@cross_origin(supports_credentials=True)
def activity_summary_for_account():
    user_info = current_user.user_config
    if user_info:

        if 'admin' in current_user.roles:
            response = ActivitySummary().for_all_orgs()
        else:
            response = ActivitySummary().for_account(user_info['account']['account_key'])
        return make_response(response), \
               {'Content-Type': 'application/json'}


@activity_summary_api.route('/organization/<organization_name>/')
@cross_origin(supports_credentials=True)
def activity_summary_for_organization(organization_name):
    return has_org_access(current_user, organization_name) and \
           make_response(ActivitySummary().for_organization(organization_name)), \
           {'Content-Type': 'application/json'}


@activity_summary_api.route('/project/<organization_name>/<project_name>/')
@cross_origin(supports_credentials=True)
def activity_summary(organization_name, project_name):
    return has_org_access(current_user, organization_name) and \
           make_response(ActivitySummary().for_project(organization_name, project_name)), \
           {'Content-Type': 'application/json'}


