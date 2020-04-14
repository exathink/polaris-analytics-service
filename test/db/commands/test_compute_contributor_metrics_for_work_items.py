# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Maneet Sehgal


from polaris.analytics.db import commands
from test.fixtures.work_items_delivery_cycles import *

class TestComputesWorkItemsContributorMetrics:

    def it_computes_contributor_metrics_for_single_delivery_cycle_for_single_work_item(self,
                                                                                         work_items_delivery_cycles_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_delivery_cycles_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_contributor_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']