# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response
from flask_cors import cross_origin
from flask_login import current_user
from polaris.analytics.datasources.activities import ActivityLevel
from .access_control import has_org_access

activity_level_api = Blueprint('activity_level_api', __name__)


@activity_level_api.route('/account/organization/')
@cross_origin(supports_credentials=True)
def activity_level_for_account_by_organization():
    user_info = current_user.user_config
    if user_info:
        if 'admin' in current_user.roles:
            response = ActivityLevel().for_all_orgs()
        else:
            response = ActivityLevel().for_account_by_organization(user_info['account']['account_key'])
        return make_response(response), \
               {'Content-Type': 'application/json'}

@activity_level_api.route('/account/project/')
@cross_origin(supports_credentials=True)
def activity_level_for_account_by_project():
    user_info = current_user.user_config
    if user_info:
        if 'admin' in current_user.roles:
            response = ActivityLevel().for_all_projects()
        else:
            response = ActivityLevel().for_account_by_project(user_info['account']['account_key'])
        return make_response(response), \
               {'Content-Type': 'application/json'}


@activity_level_api.route('/organization/project/<organization_name>/')
@cross_origin(supports_credentials=True)
def activity_level_for_organization_by_project(organization_name):
    return has_org_access(current_user, organization_name) and \
           make_response(ActivityLevel().for_organization_by_project(organization_name)), \
           {'Content-Type': 'application/json'}


@activity_level_api.route('/project/repository/<organization_name>/<project_name>/')
@cross_origin(supports_credentials=True)
def activity_level_for_project_by_repository(organization_name, project_name):
    return has_org_access(current_user, organization_name) and \
           make_response(ActivityLevel().for_project_by_repository(organization_name, project_name)), \
           {'Content-Type': 'application/json'}