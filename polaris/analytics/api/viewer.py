# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from flask_login import current_user
from polaris.common import db
from polaris.analytics.db.utils import find_or_create_account_for_user


def init_viewer_account(account_name, organization_name):
    with db.orm_session() as session:
        return find_or_create_account_for_user(current_user, account_name, organization_name)