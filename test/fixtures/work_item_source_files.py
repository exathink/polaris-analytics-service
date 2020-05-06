# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

"""
Fixture to test population of work_item_source_file_changes. Requires following tables:
repositories
commits
work_items
work_item_delivery_cycles
source_files
contributor_aliases
"""

from test.fixtures.graphql import *
from datetime import datetime, timedelta

from polaris.analytics.db.model import WorkItemDeliveryCycle, source_files

from test.fixtures.graphql import *
from datetime import datetime, timedelta

from polaris.analytics.db.model import WorkItemDeliveryCycle


earliest_commit_date = datetime.utcnow().replace(microsecond=0)-timedelta(days=5)
latest_commit_date = datetime.utcnow().replace(microsecond=0)-timedelta(days=2)
source_file_keys = [uuid.uuid4().hex, uuid.uuid4().hex]
test_contributors_info =[
    dict(
        name='Joe Blow',
        key=uuid.uuid4().hex,
        source_alias='joe@blow.com',
        source='vcs'
    ),
    dict(
        name='Ida Jay',
        key=uuid.uuid4().hex,
        source_alias='ida@jay.com',
        source='vcs'
    )
]

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


@pytest.yield_fixture()
def contributor_commits_fixture(org_repo_fixture, cleanup):
    organization, projects, repositories = org_repo_fixture
    contributor_list = []
    with db.create_session() as session:
        for test_contributor in test_contributors_info:
            contributor_id = session.connection.execute(
                contributors.insert(
                    dict(
                        key=test_contributor['key'],
                        name=test_contributor['name']
                    )
                )
            ).inserted_primary_key[0]

            contributor_alias_id = session.connection.execute(
                contributor_aliases.insert(
                    dict(
                        source_alias=test_contributor['source_alias'],
                        key=test_contributor['key'],
                        name=test_contributor['name'],
                        contributor_id=contributor_id,
                        source=test_contributor['source']
                    )
                )
            ).inserted_primary_key[0]

            contributor_list.append(dict(alias_id=contributor_alias_id, key=test_contributor['key'], name=test_contributor['name']))
    yield organization, projects, repositories, contributor_list


def commits_common_fields(contributor_commits_fixture):
    _, _, _, contributor_list = contributor_commits_fixture
    contributor = contributor_list[0]
    contributor_alias = contributor['alias_id']
    contributor_key = contributor['key']
    contributor_name = contributor['name']
    return dict(
        commit_date=datetime.utcnow(),
        commit_date_tz_offset=0,
        committer_contributor_alias_id=contributor_alias,
        committer_contributor_key=contributor_key,
        committer_contributor_name=contributor_name,
        author_date_tz_offset=0,
        author_contributor_alias_id=contributor_alias,
        author_contributor_key=contributor_key,
        author_contributor_name=contributor_name,

    )


def create_source_files(test_source_files):
    with db.create_session() as session:
        session.connection.execute(
            source_files.insert(test_source_files)
        )

@pytest.yield_fixture()
def work_items_commits_source_files_fixture(contributor_commits_fixture, cleanup):
    organization, projects, repositories, contributor_list = contributor_commits_fixture
    test_repo = repositories['alpha']

    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id=f'XXXXXX{i}',
            commit_message=f"Another change. Fixes issue {i}",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(contributor_commits_fixture)
        )
        for i in range(1000, 1005)]

    # changing commit_dates, num_parents to create test scenarios
    test_commits[0]['commit_date'] = earliest_commit_date

    # This is a merge commit
    test_commits[1]['commit_date'] = earliest_commit_date + timedelta(days=1)
    test_commits[1]['num_parents'] = 2

    # This commit shall not map to any delivery cycle
    test_commits[2]['commit_date'] = latest_commit_date

    test_commits[3]['commit_date'] = latest_commit_date + timedelta(days=1)
    test_commits[4]['commit_date'] = earliest_commit_date

    for commit in test_commits:
        commit['stats'] = {"files": 1, "lines": 8, "deletions": 4, "insertions": 4}
        commit['source_files'] = [
                    dict(
                        key=source_file_keys[0],
                        path='test/',
                        name='files1.txt',
                        file_type='txt',
                        version_count=1,
                        is_deleted=False,
                        action='A',
                        stats={"lines": 2, "insertions": 2, "deletions": 0}
                    ),
                    dict(
                        key=source_file_keys[1],
                        path='test/',
                        name='files2.py',
                        file_type='py',
                        version_count=1,
                        is_deleted=False,
                        action='A',
                        stats={"lines": 4, "insertions": 2, "deletions": 2}
                    )
                ]

    # Add commits
    create_test_commits(test_commits)

    # Add source files
    test_source_files = [
        dict(
            repository_id=test_repo.id,
            key=source_file_keys[0],
            path='test/',
            name='files1.txt',
            file_type='txt',
            version_count=1
        ),
        dict(
            repository_id=test_repo.id,
            key=source_file_keys[1],
            path='test/',
            name='files2.py',
            file_type='py',
            version_count=1
        )
    ]

    create_source_files(test_source_files)

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
            WorkItemDeliveryCycle(
                    start_seq_no=0,
                    start_date=w1.created_at,
                    end_date=latest_commit_date-timedelta(hours=1),
                    end_seq_no=2,
                    work_item_id=w1.id,
                    lead_time=int((latest_commit_date-timedelta(hours=1)-w1.created_at).total_seconds())
                ),
            WorkItemDeliveryCycle(
                    start_seq_no=3,
                    start_date=latest_commit_date+timedelta(hours=1),
                    work_item_id=w1.id
                )
        ])

        w2.delivery_cycles.extend([
            WorkItemDeliveryCycle(
                    start_seq_no=0,
                    start_date=w2.created_at,
                    work_item_id=w2.id
            )
        ])
        session.flush()

        w1.current_delivery_cycle_id = max([dc.delivery_cycle_id for dc in w1.delivery_cycles])
        w2.current_delivery_cycle_id = max([dc.delivery_cycle_id for dc in w2.delivery_cycles])

    yield organization, [w1.id, w2.id], test_commits, test_work_items, contributor_list
