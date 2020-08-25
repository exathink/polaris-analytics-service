# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.analytics.db import commands
from test.fixtures.work_items_commits import *
from polaris.utils.collections import dict_to_object


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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and repository_count=2").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and repository_count=1").scalar() == 1

        # call again
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and repository_count=2").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and repository_count=1").scalar() == 1

    def it_updates_repository_count_when_a_commit_is_mapped_to_more_than_one_work_item(self,
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[0]} and commit_count=3").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=3").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1

        # call again
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=3").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1

    def it_updates_commit_count_when_a_commit_is_mapped_to_more_than_one_work_item(self,
                                                                                   work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture

        # Map commit 2 to work item 2 resulting in increase in commit count
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
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 3
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[0]} and commit_count=3").scalar() == 1
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                            work_item_id={work_items_ids[0]} and commit_count=1").scalar() == 1
        assert db.connection().execute(f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                                    work_item_id={work_items_ids[1]} and commit_count=2").scalar() == 1

    def it_doesnt_update_commit_count_for_work_items_not_in_input(self, work_items_commits_fixture):
        organization, work_items_ids, test_commits, test_work_items = work_items_commits_fixture
        work_items_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_items[0]['key'],
                commit_key=test_commits[0]['key']
            )
        ]
        result = commands.update_work_items_commits_stats(organization.key, work_items_commits)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where \
                            work_item_id={work_items_ids[1]} and commit_count is NULL").scalar() == 1


class TestWorkItemImplementationEffort:

    def it_works_for_single_work_item_and_commit(self, implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor = fixture['contributors'][0]
        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        test_work_item = fixture['work_items'][0]
        test_commit = dict(
            key=uuid.uuid4().hex,
            source_commit_id=uuid.uuid4().hex,
            repository_id=test_repo.id,
            commit_date=datetime.utcnow(),
            **contributor['as_author'],
            **contributor['as_committer'],
            **commits_common
        )

        create_test_commits([test_commit])

        work_item_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commit['key']
            )
        ]
        add_work_item_commits(work_item_commits)

        result = commands.update_work_items_commits_stats(organization.key, work_item_commits)
        assert result['success']
        assert result['updated_work_items_effort'] == 1
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item['key']}'"
        ).scalar() == 1

        assert result['updated_delivery_cycles_effort'] == 1
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item['key']}'"
        ).scalar() == 1

    def it_works_for_single_work_item_and_multiple_commits_on_the_same_day(self, implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor = fixture['contributors'][0]
        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        test_work_item = fixture['work_items'][0]
        commit_date = datetime.utcnow()
        test_commits = [
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=commit_date,
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=commit_date,
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            )
        ]

        create_test_commits(test_commits)

        work_item_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[0]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[1]['key']
            )
        ]
        add_work_item_commits(work_item_commits)

        result = commands.update_work_items_commits_stats(organization.key, work_item_commits)
        assert result['success']
        assert result['updated_work_items_effort'] == 1

        # Effort should still be 1 since the commits are on the same day
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item['key']}'"
        ).scalar() == 1

        assert result['updated_delivery_cycles_effort'] == 1
        # Effort should still be 1 since the commits are on the same day
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item['key']}'"
        ).scalar() == 1

    def it_works_for_single_work_item_and_multiple_commits_on_different_days(self, implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor = fixture['contributors'][0]
        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        test_work_item = fixture['work_items'][0]

        test_commits = [
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=1),
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=2),
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            )
        ]

        create_test_commits(test_commits)

        work_item_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[0]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[1]['key']
            )
        ]
        add_work_item_commits(work_item_commits)

        result = commands.update_work_items_commits_stats(organization.key, work_item_commits)
        assert result['success']
        assert result['updated_work_items_effort'] == 1

        # Effort should 2 since there are two commits on 2 different days
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item['key']}'"
        ).scalar() == 2

        assert result['updated_delivery_cycles_effort'] == 1
        # Effort should 2 since there are two commits on 2 different days
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item['key']}'"
        ).scalar() == 2

    def it_works_for_single_work_item_and_commits_from_multiple_authors_on_the_same_day(self,
                                                                                        implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor_0 = fixture['contributors'][0]
        contributor_1 = fixture['contributors'][1]

        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        test_work_item = fixture['work_items'][0]
        commit_date = datetime.utcnow()

        test_commits = [
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=commit_date,
                **contributor_0['as_author'],
                **contributor_0['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=commit_date,
                **contributor_1['as_author'],
                **contributor_1['as_committer'],
                **commits_common
            )
        ]

        create_test_commits(test_commits)

        work_item_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[0]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[1]['key']
            )
        ]
        add_work_item_commits(work_item_commits)

        result = commands.update_work_items_commits_stats(organization.key, work_item_commits)
        assert result['success']
        assert result['updated_work_items_effort'] == 1

        # Effort should 2 since there are commits from 2 authors on the same days and
        # the authors have no other work item commits on the same day
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item['key']}'"
        ).scalar() == 2

        assert result['updated_delivery_cycles_effort'] == 1
        # Effort should 2 since there are commits from 2 authors on the same days and
        # the authors have no other work item commits on the same day
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item['key']}'"
        ).scalar() == 2

    def it_allocates_fractional_days_when_an_author_contributes_to_multiple_work_items_on_the_same_day \
                    (self, implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor = fixture['contributors'][0]
        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        test_work_item_0 = fixture['work_items'][0]
        test_work_item_1 = fixture['work_items'][1]

        test_commits = [
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=1),
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=2),
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=2),
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            )

        ]

        create_test_commits(test_commits)

        work_item_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_0['key'],
                commit_key=test_commits[0]['key']
            ),
            # author has commits against 2 work items on the same day
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_0['key'],
                commit_key=test_commits[1]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_1['key'],
                commit_key=test_commits[2]['key']
            )

        ]
        add_work_item_commits(work_item_commits)

        result = commands.update_work_items_commits_stats(organization.key, work_item_commits)
        assert result['success']
        assert result['updated_work_items_effort'] == 2

        # Effort for work item 0 should 1.5 since the second day is a fractional load factor
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item_0['key']}'"
        ).scalar() == 1.5

        # Effort for work item 1 should be 0.5, since on the second day the author committed to
        # two work items.
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item_1['key']}'"
        ).scalar() == 0.5

        assert result['updated_delivery_cycles_effort'] == 2

        # Effort for work item 0 should 1.5 since the second day is a fractional load factor
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item_0['key']}'"
        ).scalar() == 1.5
        # Effort for work item 1 should be 0.5, since on the second day the author committed to
        # two work items.
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item_1['key']}'"
        ).scalar() == 0.5

    def it_allocates_fractional_days_correctly_when_there_are_multiple_authors \
                    (self, implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor_0 = fixture['contributors'][0]
        contributor_1 = fixture['contributors'][1]

        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        test_work_item_0 = fixture['work_items'][0]
        test_work_item_1 = fixture['work_items'][1]

        test_commits = [
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=1),
                **contributor_0['as_author'],
                **contributor_0['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=2),
                **contributor_1['as_author'],
                **contributor_1['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=2),
                **contributor_1['as_author'],
                **contributor_1['as_committer'],
                **commits_common
            )

        ]

        create_test_commits(test_commits)

        work_item_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_0['key'],
                commit_key=test_commits[0]['key']
            ),
            # contributor_2 has commits against 2 work items on day 2
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_0['key'],
                commit_key=test_commits[1]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_1['key'],
                commit_key=test_commits[2]['key']
            )

        ]
        add_work_item_commits(work_item_commits)

        result = commands.update_work_items_commits_stats(organization.key, work_item_commits)
        assert result['success']
        assert result['updated_work_items_effort'] == 2

        # Effort for work item 0 should 1.5 since the second day is a fractional load factor for
        # contributor_2
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item_0['key']}'"
        ).scalar() == 1.5

        # Effort for work item 1 should be 0.5, since on the second day contributor_2 committed to
        # two work items.
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item_1['key']}'"
        ).scalar() == 0.5

        assert result['updated_delivery_cycles_effort'] == 2

        # Effort for work item 0 should 1.5 since the second day is a fractional load factor for contributor_2
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item_0['key']}'"
        ).scalar() == 1.5
        # Effort for work item 1 should be 0.5, since on the second day contributor_2 committed to
        # two work items.
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item_1['key']}'"
        ).scalar() == 0.5

    def it_works_for_multiple_work_items_with_multiple_commits_and_no_shared_authors \
                    (self, implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor_0 = fixture['contributors'][0]
        contributor_1 = fixture['contributors'][1]

        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        test_work_item_0 = fixture['work_items'][0]
        test_work_item_1 = fixture['work_items'][1]

        test_commits = [
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=1),
                **contributor_0['as_author'],
                **contributor_0['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=2),
                **contributor_0['as_author'],
                **contributor_0['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=1),
                **contributor_1['as_author'],
                **contributor_1['as_committer'],
                **commits_common
            ),

            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=datetime.utcnow() - timedelta(days=2),
                **contributor_1['as_author'],
                **contributor_1['as_committer'],
                **commits_common
            )

        ]

        create_test_commits(test_commits)

        work_item_commits = [
            # contributor 0 commits
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_0['key'],
                commit_key=test_commits[0]['key']
            ),

            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_0['key'],
                commit_key=test_commits[1]['key']
            ),

            # contributor 1 commits. No overlaps
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_1['key'],
                commit_key=test_commits[2]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item_1['key'],
                commit_key=test_commits[3]['key']
            )

        ]
        add_work_item_commits(work_item_commits)

        result = commands.update_work_items_commits_stats(organization.key, work_item_commits)
        assert result['success']
        assert result['updated_work_items_effort'] == 2

        # each one will have effort 2
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item_0['key']}'"
        ).scalar() == 2

        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item_1['key']}'"
        ).scalar() == 2

        assert result['updated_delivery_cycles_effort'] == 2

        # Each will have effort 2
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item_0['key']}'"
        ).scalar() == 2
        # Effort for work item 1 should be 0.5, since on the second day contributor_2 committed to
        # two work items.
        assert db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item_1['key']}'"
        ).scalar() == 2

    def it_works_for_work_items_with_multiple_delivery_cycles(self, implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor = fixture['contributors'][0]
        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        start_date = datetime.utcnow() - timedelta(days=7)

        test_work_item = fixture['work_items'][0]
        with db.orm_session() as session:
            # set up two delivery cycles
            work_item = WorkItem.find_by_work_item_key(session, test_work_item['key'])
            work_item.current_delivery_cycle.start_date = start_date
            work_item.current_delivery_cycle.end_date = start_date + timedelta(days=5)
            work_item.delivery_cycles.append(
                WorkItemDeliveryCycle(
                    start_seq_no=1,
                    start_date=start_date + timedelta(days=6),
                    work_items_source_id=work_item.work_items_source_id
                )
            )

        test_commits = [
            # goes in the first delivery cycle
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=start_date + timedelta(days=1),
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            ),
            # goes in second delivery cycle
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=start_date + timedelta(days=8),
                **contributor['as_author'],
                **contributor['as_committer'],
                **commits_common
            )
        ]

        create_test_commits(test_commits)

        work_item_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[0]['key']
            ),
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[1]['key']
            )
        ]

        add_work_item_commits(work_item_commits)

        result = commands.update_work_items_commits_stats(organization.key, work_item_commits)
        assert result['success']
        assert result['updated_work_items_effort'] == 1
        assert db.connection().execute(
            f"select effort from analytics.work_items where key='{test_work_item['key']}'"
        ).scalar() == 2

        assert result['updated_delivery_cycles_effort'] == 2
        efforts = db.connection().execute(
            f"select work_item_delivery_cycles.effort "
            f"from analytics.work_items inner join analytics.work_item_delivery_cycles "
            f"on work_items.id = work_item_delivery_cycles.work_item_id "
            f"where key='{test_work_item['key']}'"
        ).fetchall()

        assert {
            row.effort
            for row in efforts
        } == {1, 1}
