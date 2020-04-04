# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.analytics.db import commands
from test.fixtures.work_items_commits import *


class TestUpateWorkItemsCommitsSpan:

    def it_updates_commits_span_for_single_delivery_cycle_for_single_work_item(self, work_items_commits_fixture):
        # Work item has only 1 commit
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
            work_item_id={work_items_ids[1]} and earliest_commit='{test_commits[4]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1

    def it_updates_commits_span_for_work_item_with_multiple_delivery_cycles(self, work_items_commits_fixture):
        # Work item has 2 delivery cycles, first has 3 commits, second has 1
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[0]['commit_date']}' and latest_commit='{test_commits[2]['commit_date']}'").scalar() == 1
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[3]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1

    def it_updates_commits_span_for_multiple_work_items(self, work_items_commits_fixture):
        # Work item 1 has 2 delivery cycles, first has 3 commits, second has 1
        # Work item 2 has only 1 commit
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
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[0]['commit_date']}' and latest_commit='{test_commits[2]['commit_date']}'").scalar() == 1
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[3]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[1]} and earliest_commit='{test_commits[4]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1

    def it_doesnt_update_commits_span_for_work_items_not_in_input(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and earliest_commit is NULL and latest_commit is NULL").scalar() == 1

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
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[0]['commit_date']}' and latest_commit='{test_commits[2]['commit_date']}'").scalar() == 1
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[3]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[1]} and earliest_commit='{test_commits[4]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1

        # call again
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[0]['commit_date']}' and latest_commit='{test_commits[2]['commit_date']}'").scalar() == 1
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[3]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[1]} and earliest_commit='{test_commits[4]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1


    def it_updates_commits_span_when_a_commit_is_mapped_to_more_than_one_work_item(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture

        # Map commit 2 to work item 2 resulting in different earliest and latest commit dates
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
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[0]['commit_date']}' and latest_commit='{test_commits[2]['commit_date']}'").scalar() == 1
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and earliest_commit='{test_commits[3]['commit_date']}' and latest_commit=earliest_commit").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                                    work_item_id={work_items_ids[1]} and earliest_commit='{test_commits[4]['commit_date']}' and latest_commit='{test_commits[1]['commit_date']}'").scalar() == 1


class TestUpdateWorkItemsCommitsRepositoryCount:

    def it_updates_repository_count_for_single_delivery_cycle_for_single_work_item(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
            work_item_id={work_items_ids[1]} and repository_count=1").scalar() == 1

    def it_updates_repository_count_for_work_item_with_multiple_delivery_cycles(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[0]} and repository_count=1").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and repository_count=2").scalar() == 1


    def it_updates_repository_count_for_multiple_work_items(self, work_items_commits_fixture):
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
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and repository_count=2").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and repository_count=1").scalar() == 1


    def it_doesnt_update_commit_spans_for_work_items_not_in_input(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and repository_count is NULL").scalar() == 1

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
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and repository_count=2").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and repository_count=1").scalar() == 1

        # call again
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and repository_count=2").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and repository_count=1").scalar() == 1

    def it_updates_repository_count_when_a_commit_is_mapped_to_more_than_one_work_item(self, work_items_commits_fixture):
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
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and repository_count=2").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and repository_count=1").scalar() == 1
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[1]} and repository_count=2").scalar() == 1

class TestUpdateWorkItemsCommitsCount:

    def it_updates_commit_count_for_single_delivery_cycle_for_single_work_item(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
            work_item_id={work_items_ids[1]} and commit_count=1").scalar() == 1

    def it_updates_commit_count_for_work_item_with_multiple_delivery_cycles(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[0]} and commit_count=3").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1

    def it_updates_commit_count_for_multiple_work_items(self, work_items_commits_fixture):
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
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and commit_count=3").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1

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
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=3").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1

        # call again
        result = commands.compute_implementation_complexity_metrics(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=3").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1
