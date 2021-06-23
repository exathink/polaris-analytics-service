# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.analytics.db import api as db_api


def update_contributor(contributor_key, updated_info):
    return db_api.update_contributor(contributor_key, updated_info)


def update_contributor_team_assignments(organization_key, contributor_team_assignments):
    return db_api.update_contributor_team_assignments(organization_key, contributor_team_assignments)
