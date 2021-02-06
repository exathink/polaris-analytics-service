# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta
from polaris.analytics.db.model import repositories_contributor_aliases


def contributors_connection_apply_filters(select_stmt, **kwargs):
    if 'commit_within_days' in kwargs and kwargs['commit_within_days'] > 0:
        commit_from_date = datetime.utcnow() - timedelta(days=kwargs.get('commit_within_days'))
        select_stmt = select_stmt.where(
            repositories_contributor_aliases.c.latest_commit >= commit_from_date
        )
    return select_stmt


def contributor_count_apply_contributor_days_filter(select_stmt, **kwargs):
    if 'contributor_count_days' in kwargs and kwargs['contributor_count_days'] > 0:
        commit_window_start = datetime.utcnow() - timedelta(days=kwargs['contributor_count_days'])
        return select_stmt.where(
            repositories_contributor_aliases.c.latest_commit >= commit_window_start
        )
    else:
        return select_stmt
