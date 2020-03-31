# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.analytics.db import commands
from test.fixtures.work_items_commits_span import *


class TestUpateWorkItemsCommitsSpan:

    def it_updates_commits_span_for_single_delivery_cycle_for_single_work_item(self, update_work_items_commits_span_fixture):
        organization, work_items_ids, test_commits, test_work_items = update_work_items_commits_span_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[1]['key'],
                commit_key=test_commits[4]['key']
            )
        ]
        result = commands.update_work_items_commits_span(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
            work_item_id={work_items_ids[1]} and earliest_commit is not NULL").scalar() == 1

    def it_updates_commit_spans_for_work_item_with_multiple_delivery_cycles(self, update_work_items_commits_span_fixture):
        organization, work_items_ids, test_commits, test_work_items = update_work_items_commits_span_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.update_work_items_commits_span(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[0]} and earliest_commit is not NULL").scalar() == 2
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                    work_item_id={work_items_ids[0]} and earliest_commit=latest_commit").scalar() == 1

    def it_updates_commit_spans_for_multiple_work_items(self, update_work_items_commits_span_fixture):
        organization, work_items_ids, test_commits, test_work_items = update_work_items_commits_span_fixture
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
        result = commands.update_work_items_commits_span(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and earliest_commit is not NULL").scalar() == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[1]} and earliest_commit is not NULL and earliest_commit=latest_commit").scalar() == 1

    def it_doesnt_update_commit_spans_for_work_items_not_in_input(self, update_work_items_commits_span_fixture):
        organization, work_items_ids, test_commits, test_work_items = update_work_items_commits_span_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.update_work_items_commits_span(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and earliest_commit is NULL and latest_commit is NULL").scalar() == 1

    def it_is_idempotent(self, update_work_items_commits_span_fixture):
        organization, work_items_ids, test_commits, test_work_items = update_work_items_commits_span_fixture
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
        result = commands.update_work_items_commits_span(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and earliest_commit is not NULL").scalar() == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[1]} and earliest_commit is not NULL and earliest_commit=latest_commit").scalar() == 1

        # call again
        result = commands.update_work_items_commits_span(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and earliest_commit is not NULL").scalar() == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[1]} and earliest_commit is not NULL and earliest_commit=latest_commit").scalar() == 1

    def it_updates_commit_spans_when_a_commit_is_mapped_to_more_than_one_work_item(self, update_work_items_commits_span_fixture):
        organization, work_items_ids, test_commits, test_work_items = update_work_items_commits_span_fixture

        # Map commit 1 to work item 2
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
        result = commands.update_work_items_commits_span(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and earliest_commit is not NULL").scalar() == 2
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[1]} and earliest_commit is not NULL and latest_commit>earliest_commit").scalar() == 1
