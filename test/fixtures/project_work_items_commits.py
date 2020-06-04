# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import pytest
from polaris.common import db
from polaris.analytics.db.model import WorkItemDeliveryCycle, WorkItemsSource, WorkItem, WorkItemStateTransition, \
    source_files
from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.db import model
from test.fixtures.graphql import commits_fixture, org_repo_fixture, get_date, create_test_commits, \
    create_work_item_commits, commits_common_fields
from test.constants import *

source_file_keys = [uuid.uuid4().hex, uuid.uuid4().hex]


def create_source_files(test_source_files):
    with db.create_session() as session:
        session.connection.execute(
            source_files.insert(test_source_files)
        )


@pytest.yield_fixture()
def project_work_items_commits_fixture(commits_fixture):
    organization, projects, repositories, contributor = commits_fixture
    test_repo = repositories['alpha']
    project = organization.projects[0]

    work_items_common = dict(

        name='Issue 10',
        display_id='1000',
        created_at=get_date("2020-01-02"),
        updated_at=get_date("2020-01-03"),
        is_bug=True,
        work_item_type='issue',
        url='http://foo.com',
        tags=['ares2'],
        description='foo',
        source_id=str(uuid.uuid4()),
    )

    delivery_cycles = [
        dict(
            start_seq_no=0,
            start_date=get_date("2020-01-04")
        )
    ]
    # open state_type of this new work item should be updated to complete after the test
    new_work_items = [
        dict(
            key=uuid.uuid4().hex,
            **work_items_common,
            state='done'
        )
    ]

    work_items_state_transitions = [
        dict(
            seq_no=0,
            created_at=get_date("2020-01-04"),
            state='created',
            previous_state=None
        ),
        dict(
            seq_no=1,
            created_at=get_date("2020-01-05"),
            state='doing',
            previous_state='created'
        ),
        dict(
            seq_no=2,
            created_at=get_date("2020-01-07"),
            state='done',
            previous_state='doing'
        )

    ]

    with db.orm_session() as session:
        session.expire_on_commit = False
        session.add(organization)
        session.add(project)
        project.work_items_sources.append(
            WorkItemsSource(
                organization_key=str(organization.key),
                organization_id=organization.id,
                key=str(uuid.uuid4()),
                name='foo',
                integration_type='jira',
                work_items_source_type='repository_issues',
                commit_mapping_scope='repository',
                source_id=str(uuid.uuid4())
            )
        )
        session.flush()
        work_items_source = project.work_items_sources[0]
        work_items_source.init_state_map(
            [
                dict(state='created', state_type=WorkItemsStateType.open.value),
                dict(state='doing', state_type=WorkItemsStateType.wip.value),
                dict(state='done', state_type=WorkItemsStateType.wip.value)
            ]
        )
        work_items_source.work_items.extend([
            WorkItem(**item)
            for item in new_work_items
        ])

        work_items_source.work_items[0].delivery_cycles.extend([
            WorkItemDeliveryCycle(work_items_source_id=work_items_source.id, **cycle)
            for cycle in delivery_cycles
        ])

        work_items_source.work_items[0].state_transitions.extend([
            WorkItemStateTransition(**transition)
            for transition in work_items_state_transitions
        ])

        work_items_source.work_items[0].delivery_cycles[0].delivery_cycle_durations.extend([
            model.WorkItemDeliveryCycleDuration(
                state='created',
                cumulative_time_in_state=None  # setting None, should be updated by test
            ),
            model.WorkItemDeliveryCycleDuration(
                state='doing',
                cumulative_time_in_state=None
            ),
            model.WorkItemDeliveryCycleDuration(
                state='done',
                cumulative_time_in_state=None
            )
        ])

    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id=f'XXXXXX{i}',
            commit_message=f"Another change. Fixes issue {i}",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
        for i in range(1000, 1003)]

    # changing commit_dates, c1-c4 for w1
    test_commits[0]['commit_date'] = get_date("2020-01-05")
    test_commits[1]['commit_date'] = get_date("2020-01-06")
    test_commits[2]['commit_date'] = get_date("2020-01-07")

    # changing repo ids to test repository count
    test_commits[1]['repository_id'] = repositories['beta'].id
    test_commits[2]['repository_id'] = repositories['gamma'].id

    # updating test commits for commits stats, 1 indicates non merge commits, >1 is merge commit
    test_commits[0]['num_parents'] = 1
    test_commits[1]['num_parents'] = 2
    test_commits[2]['num_parents'] = 1

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

    # Add work item commits mapping
    create_work_item_commits(new_work_items[0]['key'], [commit['key'] for commit in test_commits[0:3]])

    yield project
