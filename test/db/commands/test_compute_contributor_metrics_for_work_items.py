# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.analytics.db import commands
from test.fixtures.work_items_commits_contributors import *

class TestComputeContributorMetricsForWorkItems:

    def it_computes_contributor_metrics_for_single_delivery_cycle_work_item_with_single_contributor(self, work_items_commits_contributors_fixture):
        organization, work_items_ids, test_commits, test_work_items, contributor_list = work_items_commits_contributors_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_contributor_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 1
        assert db.connection().execute("select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where total_lines_as_author=8 and total_lines_as_reviewer=0").scalar() == 1

    def it_computes_contributor_metrics_for_single_work_item_multiple_delivery_cycles_multiple_contributors(self, work_items_commits_contributors_fixture):
        organization, work_items_ids, test_commits, test_work_items, contributor_list = work_items_commits_contributors_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_contributor_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 4
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=16 and total_lines_as_reviewer=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=0").scalar() == 1

    def it_computes_contributor_metrics_for_multiple_work_items_multiple_delivery_cycles_multiple_contributors(self,
                                                                                                            work_items_commits_contributors_fixture):
        organization, work_items_ids, test_commits, test_work_items, contributor_list = work_items_commits_contributors_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_contributor_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 5
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=16 and total_lines_as_reviewer=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=8 and total_lines_as_reviewer=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[1]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=8").scalar() == 2


    def it_computes_contributor_metrics_when_single_commit_maps_multiple_work_items(self, work_items_commits_contributors_fixture):
        organization, work_items_ids, test_commits, test_work_items, contributor_list = work_items_commits_contributors_fixture

        # Map commit 3 to work item 2 resulting in increase in
        create_work_item_commits(test_work_items[1]['key'], [test_commits[2]['key']])
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_contributor_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 6
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=16 and total_lines_as_reviewer=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=16 and total_lines_as_reviewer=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[1]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=8").scalar() == 3

    def it_is_idempotent(self, work_items_commits_contributors_fixture):
        organization, work_items_ids, test_commits, test_work_items, contributor_list = work_items_commits_contributors_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_contributor_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 5
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=16 and total_lines_as_reviewer=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=8 and total_lines_as_reviewer=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[1]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=8").scalar() == 2

        # Repeat with same input
        result = commands.compute_contributor_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 5
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=16 and total_lines_as_reviewer=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[0]['alias_id']} and total_lines_as_author=8 and total_lines_as_reviewer=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[1]['alias_id']} and total_lines_as_author=0 and total_lines_as_reviewer=8").scalar() == 2

    def it_does_not_compute_contributor_metrics_for_contributor_not_linked_to_commits(self, work_items_commits_contributors_fixture):
        organization, work_items_ids, test_commits, test_work_items, contributor_list = work_items_commits_contributors_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_contributor_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors where contributor_alias_id={contributor_list[1]['alias_id']}").scalar() == 0
