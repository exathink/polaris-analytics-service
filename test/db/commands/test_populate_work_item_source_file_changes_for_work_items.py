# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.analytics.db import commands
from test.fixtures.work_item_source_files import *


class TestPopulateSourceFileChangesForWorkItems:

    def it_populates_source_file_changes_for_one_work_item_commit_one_delivey_cycle_one_contributor(self,
                                                                                                    work_items_commits_source_files_fixture):
        organization, _, test_commits, test_work_items, _ = work_items_commits_source_files_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.populate_work_items_source_file_changes_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute("select count(*) from analytics.work_item_source_file_changes").scalar() == 2
        # Check that each source file change is populated, also delivery cycle should be non null
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 2

    def it_populates_source_file_changes_for_multiple_work_items_commits_multiple_delivery_cycles_multiple_contributors(
            self, work_items_commits_source_files_fixture):
        organization, _, test_commits, test_work_items, _ = work_items_commits_source_files_fixture
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
        result = commands.populate_work_items_source_file_changes_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 4
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 4

    def it_populates_source_file_changes_for_commits_not_associated_with_delivery_cycle_id(self,
                                                                                           work_items_commits_source_files_fixture):
        organization, _, test_commits, test_work_items, _ = work_items_commits_source_files_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[2]['key']
            )
        ]
        result = commands.populate_work_items_source_file_changes_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is NULL").scalar() == 2

    def it_populates_source_file_changes_for_all_types_of_work_items_commits(self,
                                                                             work_items_commits_source_files_fixture):
        organization, _, test_commits, test_work_items, _ = work_items_commits_source_files_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[1]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[2]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[3]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.populate_work_items_source_file_changes_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 10
        # Check for delivery cycle non null cases
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 8
        # Check for delivery cycle null case
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is NULL").scalar() == 2

    def it_populates_file_changes_when_a_single_work_item_commit_with_delivery_cycle_maps_to_multiple_work_items(self,
                                                                                                                 work_items_commits_source_files_fixture):
        organization, _, test_commits, test_work_items, _ = work_items_commits_source_files_fixture

        # Map commit 2 to work item 2
        create_work_item_commits(test_work_items[1]['key'], [test_commits[1]['key']])
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[1]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[1]['key']
            )
        ]
        result = commands.populate_work_items_source_file_changes_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 4
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 4

    def it_populates_file_changes_when_a_single_work_item_commit_without_delivery_cycle_maps_to_multiple_work_items(
            self, work_items_commits_source_files_fixture):
        organization, _, test_commits, test_work_items, _ = work_items_commits_source_files_fixture

        # Map commit 3 to work item 2 which also has an open delivery cycle, so for w1 its out of delivery cycle but for w2 its within
        create_work_item_commits(test_work_items[1]['key'], [test_commits[2]['key']])
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[2]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[2]['key']
            )
        ]
        result = commands.populate_work_items_source_file_changes_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 4
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 2
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is NULL").scalar() == 2

    def it_is_idempotent(self, work_items_commits_source_files_fixture):
        organization, _, test_commits, test_work_items, _ = work_items_commits_source_files_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.populate_work_items_source_file_changes_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute("select count(*) from analytics.work_item_source_file_changes").scalar() == 2
        # Check that each source file change is populated, also delivery cycle should be non null
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 2
        # Repeat
        result = commands.populate_work_items_source_file_changes_for_work_items(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute("select count(*) from analytics.work_item_source_file_changes").scalar() == 2
        # Check that each source file change is populated, also delivery cycle should be non null
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 2
