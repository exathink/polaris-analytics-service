# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.analytics.db import api as db_api


def update_contributor_for_contributor_aliases(contributor_key, contributor_alias_keys):
    return db_api.update_contributor_for_contributor_aliases(contributor_key, contributor_alias_keys)
