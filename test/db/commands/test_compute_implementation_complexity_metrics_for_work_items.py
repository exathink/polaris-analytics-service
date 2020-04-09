# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.analytics.db import commands
from test.fixtures.work_items_commits import *


class TestUpdateWorkItemsNonMergeCommitsStats:

    def it_updates_non_merge_commit_stats_for_single_delivery_cycle_for_single_work_item(self,
                                                                                         work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
            work_item_id={work_items_ids[1]} and total_lines_changed_non_merge=8 and total_files_changed_non_merge=1 and total_lines_deleted_non_merge=4 and total_lines_inserted_non_merge=4").scalar() == 1

    def it_updates_non_merge_commit_stats_for_work_item_with_multiple_delivery_cycles(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=0 and total_files_changed_non_merge=0 and total_lines_deleted_non_merge=0 and total_lines_inserted_non_merge=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=16 and total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 and total_lines_inserted_non_merge=8").scalar() == 1

    def it_updates_non_merge_commit_stats_for_multiple_work_items(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
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
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=0 and total_files_changed_non_merge=0 and total_lines_deleted_non_merge=0 and total_lines_inserted_non_merge=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=16 and total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 and total_lines_inserted_non_merge=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[1]} and total_lines_changed_non_merge=8 and total_files_changed_non_merge=1 and total_lines_deleted_non_merge=4 and total_lines_inserted_non_merge=4").scalar() == 1

    def it_doesnt_update_non_merge_commit_stats_for_work_items_not_in_input(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and total_lines_changed_non_merge is NULL and total_files_changed_non_merge is NULL and total_lines_deleted_non_merge is NULL and total_lines_inserted_non_merge is NULL").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=0 and total_files_changed_non_merge=0 and total_lines_deleted_non_merge=0 and total_lines_inserted_non_merge=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=16 and total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 and total_lines_inserted_non_merge=8").scalar() == 1


    def it_is_idempotent(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
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
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                                work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=0 and total_files_changed_non_merge=0 and total_lines_deleted_non_merge=0 and total_lines_inserted_non_merge=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                                        work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=16 and total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 and total_lines_inserted_non_merge=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                work_item_id={work_items_ids[1]} and total_lines_changed_non_merge=8 and total_files_changed_non_merge=1 and total_lines_deleted_non_merge=4 and total_lines_inserted_non_merge=4").scalar() == 1

        # call again
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                                work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=0 and total_files_changed_non_merge=0 and total_lines_deleted_non_merge=0 and total_lines_inserted_non_merge=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                                        work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=16 and total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 and total_lines_inserted_non_merge=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                work_item_id={work_items_ids[1]} and total_lines_changed_non_merge=8 and total_files_changed_non_merge=1 and total_lines_deleted_non_merge=4 and total_lines_inserted_non_merge=4").scalar() == 1


    def it_updates_non_merge_commit_stats_when_a_commit_is_mapped_to_more_than_one_work_item(self,
                                                                                             work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture

        # Map commit 3 to work item 2 resulting in increase in repo count
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
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                                work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=0 and total_files_changed_non_merge=0 and total_lines_deleted_non_merge=0 and total_lines_inserted_non_merge=0").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                                        work_item_id={work_items_ids[0]} and total_lines_changed_non_merge=16 and total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 and total_lines_inserted_non_merge=8").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                work_item_id={work_items_ids[1]} and total_lines_changed_non_merge=16 and total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 and total_lines_inserted_non_merge=8").scalar() == 1

class TestUpdateWorkItemsMergeCommitsStats:

    def it_updates_merge_commit_stats_for_single_delivery_cycle_for_single_work_item(self,
                                                                                         work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
            work_item_id={work_items_ids[1]} and total_lines_changed_merge=0 and total_files_changed_merge=0 and average_lines_changed_merge=0").scalar() == 1

    def it_updates_merge_commit_stats_for_work_item_with_multiple_delivery_cycles(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[0]} and total_lines_changed_merge=8 and total_files_changed_merge=1 and average_lines_changed_merge=8").scalar() == 2

    def it_updates_merge_commit_stats_for_multiple_work_items(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
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
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and total_lines_changed_merge=8 and total_files_changed_merge=1 and average_lines_changed_merge=8").scalar() == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[1]} and total_lines_changed_merge=0 and total_files_changed_merge=0 and average_lines_changed_merge=0").scalar() == 1

    def it_doesnt_update_merge_commit_stats_for_work_items_not_in_input(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and total_lines_changed_merge is NULL and total_files_changed_merge is NULL and average_lines_changed_merge is NULL").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and total_lines_changed_merge=8 and total_files_changed_merge=1 and average_lines_changed_merge=8").scalar() == 2

    def it_is_idempotent(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
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
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and total_lines_changed_merge=8 and total_files_changed_merge=1 and average_lines_changed_merge=8").scalar() == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and total_lines_changed_merge=0 and total_files_changed_merge=0 and average_lines_changed_merge=0").scalar() == 1

        # call again
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and total_lines_changed_merge=8 and total_files_changed_merge=1 and average_lines_changed_merge=8").scalar() == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and total_lines_changed_merge=0 and total_files_changed_merge=0 and average_lines_changed_merge=0").scalar() == 1


    def it_updates_merge_commit_stats_when_a_commit_is_mapped_to_more_than_one_work_item(self,
                                                                                             work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture

        # Map commit 2 to work item 2 resulting in increase in repo count
        create_work_item_commits(test_work_items[1]['key'], [test_commits[1]['key']])
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
        result = commands.compute_implementation_complexity_metrics_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and total_lines_changed_merge=8 and total_files_changed_merge=1 and average_lines_changed_merge=8").scalar() == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and total_lines_changed_merge=8 and total_files_changed_merge=1 and average_lines_changed_merge=8").scalar() == 1
