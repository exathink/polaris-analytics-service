# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from test.fixtures.repo_org import rails_repository_key
from datetime import datetime


def pull_requests_common():
    return dict(
        title="PO-178 Graphql API updates.",
        description="PO-178",
        source_state="open",
        state="open",
        created_at=datetime.strptime("2020-06-18 01:32:00.553000", "%Y-%m-%d %H:%M:%S.%f"),
        updated_at=datetime.strptime("2020-06-23 01:53:48.171000", "%Y-%m-%d %H:%M:%S.%f"),
        merge_status="can_be_merged",
        end_date=datetime.strptime("2020-06-11 18:57:08.818000", "%Y-%m-%d %H:%M:%S.%f"),
        source_branch="PO-178",
        target_branch="master",
        source_repository_key=rails_repository_key,
        web_url="https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
    )
