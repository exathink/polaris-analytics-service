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

from .access_control import has_org_access, has_project_access

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


@activity_summary_api.route('/organization/<organization_key>/')
@cross_origin(supports_credentials=True)
def activity_summary_for_organization(organization_key):
    return has_org_access(current_user, organization_key) and \
           make_response(ActivitySummary().for_organization(organization_key)), \
           {'Content-Type': 'application/json'}


@activity_summary_api.route('/project/<project_key>/')
@cross_origin(supports_credentials=True)
def activity_summary(project_key):
    return has_project_access(current_user, project_key) and \
           make_response(ActivitySummary().for_project(project_key)), \
           {'Content-Type': 'application/json'}


