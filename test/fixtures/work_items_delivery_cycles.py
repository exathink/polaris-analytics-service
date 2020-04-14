# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Maneet Sehgal


# Fixture to test computation of work items contributor metrics

import pytest
from test.fixtures.graphql import *
from datetime import datetime, timedelta

from polaris.analytics.db.model import WorkItemDeliveryCycles

earliest_commit_date = datetime.utcnow().replace(microsecond=0)-timedelta(days=5)
latest_commit_date = datetime.utcnow().replace(microsecond=0)-timedelta(days=2)


test_work_items = [
    dict(
        key=uuid.uuid4().hex,
        name='Issue 1',
        display_id='2000',
        created_at=datetime.utcnow().replace(microsecond=0) - timedelta(days=7),
        updated_at=datetime.utcnow().replace(microsecond=0) - timedelta(days=7),
        **work_items_common
    )
    for i in range(0, 2)
]


@pytest.yield_fixture()
def work_items_delivery_cycles_fixture(commits_fixture):
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
    create_work_item_commits(test_work_items[0]['key'], [commit['key'] for commit in test_commits[0:4]])
    create_work_item_commits(test_work_items[1]['key'], [test_commits[-1]['key']])

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
            WorkItemDeliveryCycles(
                    start_seq_no=0,
                    start_date=w1.created_at,
                    end_date=latest_commit_date,
                    end_seq_no=2,
                    work_item_id=w1.id,
                    lead_time=int((datetime.utcnow()-timedelta(hours=1)-w1.created_at).total_seconds())
                ),
            WorkItemDeliveryCycles(
                    start_seq_no=3,
                    start_date=latest_commit_date+timedelta(hours=1),
                    work_item_id=w1.id
                )
        ])

        w2.delivery_cycles.extend([
            WorkItemDeliveryCycles(
                    start_seq_no=0,
                    start_date=w2.created_at,
                    work_item_id=w2.id,
                    total_lines_changed_non_merge= 10,
                    total_files_changed_non_merge =  2,
                    total_lines_deleted_non_merge =  8,
                    total_lines_inserted_non_merge = 1
            )
        ])
        session.flush()

        w1.current_delivery_cycle_id = max([dc.delivery_cycle_id for dc in w1.delivery_cycles])
        w2.current_delivery_cycle_id = max([dc.delivery_cycle_id for dc in w2.delivery_cycles])

    yield organization, [w1.id, w2.id], test_commits, test_work_items
