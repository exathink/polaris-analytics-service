# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


"""
# Fixture to test update of work items commits span i.e. earliest commit and latest commit dates

# Scenario
1. 2 work items: w1, w2, 5 commits: w1c1, w1c2, w1c3, w1c4, w2c1
    3 delivery cycles: w1d1, w1d2, w2d1
Expected result: All 3 delivery cycles are populated with commit dates, w1d2 and w2d1 will have earliest and latest same
"""

import pytest
from test.fixtures.graphql import *
from datetime import datetime, timedelta

from polaris.analytics.db.model import WorkItemDeliveryCycle, work_item_delivery_cycle_contributors
from sqlalchemy.dialects.postgresql import insert

earliest_commit_date = datetime.utcnow().replace(microsecond=0) - timedelta(days=5)
latest_commit_date = datetime.utcnow().replace(microsecond=0) - timedelta(days=2)

test_work_items = [
    dict(
        key=uuid.uuid4().hex,
        name='Issue 1',
        display_id='1000',
        created_at=datetime.utcnow().replace(microsecond=0) - timedelta(days=7),
        updated_at=datetime.utcnow().replace(microsecond=0) - timedelta(days=7),
        **work_items_common
    )
    for i in range(0, 2)
]


@pytest.fixture()
def work_items_commits_fixture(commits_fixture):
    organization, projects, repositories, contributor = commits_fixture
    test_repo = repositories['alpha']

    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id=f'XXXXXX{i}',
            commit_message=f"Another change. Fixes issue {i}",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
        for i in range(1000, 1005)]

    # changing commit_dates, c1-c4 for w1
    test_commits[0]['commit_date'] = earliest_commit_date
    test_commits[1]['commit_date'] = earliest_commit_date + timedelta(days=1)
    test_commits[2]['commit_date'] = latest_commit_date
    test_commits[3]['commit_date'] = latest_commit_date + timedelta(days=1)
    test_commits[4]['commit_date'] = earliest_commit_date

    # changing repo ids to test repository count
    test_commits[1]['repository_id'] = repositories['beta'].id
    test_commits[4]['repository_id'] = repositories['gamma'].id

    # updating test commits for commits stats, 1 indicates non merge commits, >1 is merge commit
    test_commits[0]['num_parents'] = 1
    test_commits[1]['num_parents'] = 2
    test_commits[2]['num_parents'] = 1
    test_commits[3]['num_parents'] = 2
    test_commits[4]['num_parents'] = 1

    for commit in test_commits:
        commit['stats'] = {"files": 1, "lines": 8, "deletions": 4, "insertions": 4}

    # Add commits
    create_test_commits(test_commits)

    # Add work items
    create_work_items(
        organization,
        source_data=dict(
            integration_type='github',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=test_repo.key,
            **work_items_source_common
        ),
        items_data=test_work_items
    )

    # Add work item commits mapping

    # Add state transitions
    for work_item in test_work_items:
        create_transitions(work_item['key'], [
            dict(
                seq_no=0,
                previous_state=None,
                state='backlog',
                created_at=work_item['created_at']
            ),
            dict(
                seq_no=1,
                previous_state=None,
                state='open',
                created_at=work_item['created_at']
            )
        ])
    # Add more transitions for w1
    create_transitions(test_work_items[0]['key'], [
        dict(
            seq_no=2,
            previous_state='open',
            state='closed',
            created_at=datetime.utcnow() - timedelta(hours=1)
        ),
        dict(
            seq_no=3,
            previous_state='closed',
            state='open',
            created_at=datetime.utcnow()
        ),
    ])

    # Add delivery cycles
    with db.orm_session() as session:
        w1 = WorkItem.find_by_work_item_key(session, test_work_items[0]['key'])
        w2 = WorkItem.find_by_work_item_key(session, test_work_items[1]['key'])

        w1.delivery_cycles.extend([
            WorkItemDeliveryCycle(
                work_items_source_id=w1.work_items_source_id,
                start_seq_no=0,
                start_date=w1.created_at,
                end_date=latest_commit_date,
                end_seq_no=2,
                work_item_id=w1.id,
                lead_time=int((datetime.utcnow() - timedelta(hours=1) - w1.created_at).total_seconds())
            ),
            WorkItemDeliveryCycle(
                work_items_source_id=w1.work_items_source_id,
                start_seq_no=3,
                start_date=latest_commit_date + timedelta(hours=1),
                work_item_id=w1.id
            )
        ])

        w2.delivery_cycles.extend([
            WorkItemDeliveryCycle(
                work_items_source_id=w2.work_items_source_id,
                start_seq_no=0,
                start_date=w2.created_at,
                work_item_id=w2.id
            )
        ])
        session.flush()

        w1.current_delivery_cycle_id = max([dc.delivery_cycle_id for dc in w1.delivery_cycles])
        w2.current_delivery_cycle_id = max([dc.delivery_cycle_id for dc in w2.delivery_cycles])

    create_work_item_commits(test_work_items[0]['key'], [commit['key'] for commit in test_commits[0:4]])
    create_work_item_commits(test_work_items[1]['key'], [test_commits[-1]['key']])

    yield organization, [w1.id, w2.id], test_commits, test_work_items


@pytest.fixture()
def implementation_effort_commits_fixture(org_repo_fixture, cleanup):
    organization, projects, repositories = org_repo_fixture
    contributors_fixture = []
    with db.create_session() as session:
        for i in range(0, 3):
            contributor_key = uuid.uuid4().hex
            contributor_name = str(i)
            contributor_id = session.connection.execute(
                contributors.insert(
                    dict(
                        key=contributor_key,
                        name=contributor_name
                    )
                )
            ).inserted_primary_key[0]

            contributor_alias_id = session.connection.execute(
                contributor_aliases.insert(
                    dict(
                        source_alias=f'${i}@blow.com',
                        key=contributor_key,
                        name=contributor_name,
                        contributor_id=contributor_id,
                        source='vcs'
                    )
                )
            ).inserted_primary_key[0]

            # this fixture is designed to make it easy to
            # setup a contributor as an author or a committer
            # of a commit
            contributors_fixture.append(
                dict(
                    as_author=dict(
                        author_contributor_alias_id=contributor_alias_id,
                        author_contributor_key=contributor_key,
                        author_contributor_name=contributor_name
                    ),
                    as_committer=dict(
                        committer_contributor_alias_id=contributor_alias_id,
                        committer_contributor_key=contributor_key,
                        committer_contributor_name=contributor_name
                    )
                )
            )

    yield organization, projects, repositories, contributors_fixture


def update_delivery_cycle_commit_info(work_item_key, commit_keys):
    with db.orm_session() as session:
        work_item = WorkItem.find_by_work_item_key(session, work_item_key)
        if work_item:
            for commit_key in commit_keys:
                commit = Commit.find_by_commit_key(session, commit_key)
                if commit:
                    delivery_cycle = find(
                        work_item.delivery_cycles,
                        lambda dc:
                        # This is the old date based rule we were using
                        # to map delivery cycles to commits.
                        # Not using current_delivery_cycle_id as that may not be updated
                        dc.start_date <= commit.commit_date and
                        (dc.end_date is None or commit.commit_date <= dc.end_date)
                    )
                    # Update delivery_cycle table with latest and earliest commit
                    if delivery_cycle:
                        if delivery_cycle.latest_commit == None or commit.commit_date > delivery_cycle.latest_commit:
                            delivery_cycle.latest_commit = commit.commit_date
                        if delivery_cycle.earliest_commit == None or commit.commit_date < delivery_cycle.earliest_commit:
                            delivery_cycle.earliest_commit = commit.commit_date
                        # create entry in work_item_delivery_cycle_contributors
                        stmt = insert(work_item_delivery_cycle_contributors).values(
                            delivery_cycle_id=delivery_cycle.delivery_cycle_id,
                            contributor_alias_id=commit.committer_contributor_alias_id,
                            total_lines_as_author=10,
                            total_lines_as_reviewer=20
                        )
                        stmt = stmt.on_conflict_do_update(
                            index_elements=[
                                work_item_delivery_cycle_contributors.c.delivery_cycle_id,
                                work_item_delivery_cycle_contributors.c.contributor_alias_id
                            ],
                            set_=dict(
                                total_lines_as_author=stmt.excluded.total_lines_as_author,
                                total_lines_as_reviewer=stmt.excluded.total_lines_as_reviewer
                            )
                        )
                        session.connection().execute(stmt)


@pytest.fixture()
def implementation_effort_fixture(implementation_effort_commits_fixture):
    def add_work_item_commits(work_item_commits):
        for entry in work_item_commits:
            # shimming this to call the standard fixture function
            create_work_item_commits(entry['work_item_key'], [entry['commit_key']])
            # update delivery cycle with earliest and latest commit
            update_delivery_cycle_commit_info(entry['work_item_key'], [entry['commit_key']])

    organization, projects, repositories, contributors = implementation_effort_commits_fixture

    test_work_items = [
        dict(
            key=uuid.uuid4().hex,
            name=f'Issue ${i}',
            display_id=f'100${i}',
            created_at=datetime.utcnow().replace(microsecond=0) - timedelta(days=7),
            updated_at=datetime.utcnow().replace(microsecond=0) - timedelta(days=7),
            **work_items_common
        )
        for i in range(0, 5)
    ]

    create_work_items_with_default_delivery_cycle(
        organization,
        source_data=dict(
            integration_type='github',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=uuid.uuid4().hex,
            **work_items_source_common
        ),
        items_data=test_work_items
    )

    commits_common = dict(

        commit_message=f"Another change. Fixes nothing",
        author_date=get_date("2018-12-03"),
        commit_date_tz_offset=0,
        author_date_tz_offset=0,
    )

    yield dict(
        organization=organization,
        projects=projects,
        repositories=repositories,
        contributors=contributors,
        work_items=test_work_items,
        commits_common=commits_common,
        add_work_item_commits=add_work_item_commits
    )
