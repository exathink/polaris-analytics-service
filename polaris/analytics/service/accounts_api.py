# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response
from flask_cors import cross_origin
from flask_login import current_user
from polaris.analytics.datasources.accounts import \
    ActivitySummary, \
    ActivitySummaryByOrganization


accounts_api = Blueprint('accounts_api', __name__)


@accounts_api.route('/activity-summary-by-organization/')
@cross_origin(supports_credentials=True)
def activity_summary_by_organization():
    user_info = current_user.user_config
    if user_info:
        activity_summary = ActivitySummaryByOrganization()
        if 'admin' in current_user.roles:
            response = activity_summary.for_all_orgs()
        else:
            response = activity_summary.for_account(user_info['account']['account_key'])
        return make_response(response), \
               {'Content-Type': 'application/json'}


@accounts_api.route('/activity-summary/')
@cross_origin(supports_credentials=True)
def activity_summary():
    user_info = current_user.user_config
    if user_info:
        account_summary = ActivitySummary()
        if 'admin' in current_user.roles:
            response = account_summary.for_all_orgs()
        else:
            response = account_summary.for_account(user_info['account']['account_key'])
        return make_response(response), \
               {'Content-Type': 'application/json'}

