# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.utils.collections import Fixture
from polaris.analytics.db.model import Project, WorkItemsSource, WorkItem, WorkItemDeliveryCycle, \
    WorkItemStateTransition
from polaris.analytics.db.enums import WorkItemsStateType
from test.fixtures.graphql import get_date

from test.fixtures.repo_org import *
from test.constants import *

test_projects = [
    dict(name='mercury', key=uuid.uuid4()),
    dict(name='venus', key=uuid.uuid4())
]


@pytest.fixture()
def setup_projects(setup_org):
    organization = setup_org
    for project in test_projects:
        with db.orm_session() as session:
            session.expire_on_commit = False
            session.add(organization)
            organization.projects.append(
                Project(
                    name=project['name'],
                    key=project['key']
                )
            )

    yield organization


@pytest.fixture()
def setup_work_items_sources(setup_projects):
    organization = setup_projects
    project = organization.projects[0]
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
    yield project


class ProjectWorkItemsTest:

    @pytest.fixture()
    def setup(self, setup_work_items_sources):
        with db.orm_session() as session:
            project = setup_work_items_sources
            session.add(project)

            organization=project.organization
            work_items_source = project.work_items_sources[0]
            work_items_common = dict(
                state='doing',
                created_at=get_date("2018-12-02"),
                updated_at=get_date("2018-12-03"),
                is_bug=False,
                is_epic=False,
                work_item_type='issue',
                url='http://foo.com',
                tags=['ares2'],
                description='foo',
                source_id=str(uuid.uuid4()),
            )

        yield Fixture(
            organization=organization,
            project=project,
            work_items_source=work_items_source,
            work_items_common=work_items_common
        )


@pytest.fixture()
def setup_work_items(setup_projects):
    organization = setup_projects
    project = organization.projects[0]
    work_items_common = dict(

        name='Issue 10',
        display_id='1000',
        created_at=get_date("2018-12-02"),
        updated_at=get_date("2018-12-03"),
        is_bug=True,
        work_item_type='issue',
        url='http://foo.com',
        tags=['ares2'],
        description='foo',
        source_id=str(uuid.uuid4()),
    )

    # open state_type of this new work item should be updated to complete after the test
    new_work_items = [
        dict(
            key=uuid.uuid4().hex,
            **work_items_common,
            state='todo'
        ),
        dict(
            key=uuid.uuid4().hex,
            **work_items_common,
            state='doing'
        ),
        dict(
            key=uuid.uuid4().hex,
            **work_items_common,
            state='done'
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
        project.work_items_sources[0].work_items.extend([
            WorkItem(**item)
            for item in new_work_items
        ])

    yield project


@pytest.fixture()
def work_items_delivery_cycles_setup(setup_projects):
    organization = setup_projects
    project = organization.projects[0]
    work_items_common = dict(

        name='Issue 10',
        display_id='1000',
        created_at=get_date("2018-12-02"),
        updated_at=get_date("2018-12-03"),
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
            start_date=get_date("2020-03-19")
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
            created_at=get_date("2020-03-20"),
            state='created',
            previous_state=None
        ),
        dict(
            seq_no=1,
            created_at=get_date("2020-03-20"),
            state='doing',
            previous_state='created'
        ),
        dict(
            seq_no=2,
            created_at=get_date("2020-03-21"),
            state='done',
            previous_state='doing'
        ),

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
        session.flush()

        work_items_source.work_items[0].current_delivery_cycle_id = work_items_source.work_items[0].delivery_cycles[0].delivery_cycle_id

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

    yield project
