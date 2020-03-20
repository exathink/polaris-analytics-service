# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
import uuid
from datetime import datetime
from polaris.common import db
from polaris.analytics.db.model import Project, WorkItemsSource, WorkItem, WorkItemDeliveryCycles, \
    WorkItemStateTransition
from graphene.test import Client
from polaris.analytics.service.graphql import schema
from polaris.utils.collections import find
from polaris.analytics.db.enums import WorkItemsStateType
from test.fixtures.graphql import get_date, work_items_common

from test.fixtures.repo_org import *
from test.constants import *
from polaris.analytics import api

test_projects = [
    dict(name='mercury', key=uuid.uuid4()),
    dict(name='venus', key=uuid.uuid4())
]


@pytest.yield_fixture()
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


@pytest.yield_fixture()
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


class TestArchiveProject:

    def it_archives_a_project(self, setup_projects):
        client = Client(schema)

        response = client.execute("""
            mutation archiveProject($archiveProjectInput: ArchiveProjectInput!) {
                archiveProject(archiveProjectInput:$archiveProjectInput) {
                    projectName
                }
            }
        """, variable_values=dict(
            archiveProjectInput=dict(
                projectKey=str(test_projects[0]['key'])
            )
        ))
        assert response['data']['archiveProject']['projectName'] == 'mercury'
        assert db.connection().execute(
            f"select archived from analytics.projects where key='{test_projects[0]['key']}'"
        ).scalar()


class TestUpdateProjectStateMaps:

    def it_updates_project_work_items_source_state_map(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        response = client.execute("""
            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                        success
                    }
                }
        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="created", stateType=WorkItemsStateType.backlog.value),
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value),
                            dict(state="closed", stateType=WorkItemsStateType.closed.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']

    def it_checks_if_project_exists(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(uuid.uuid4())
        work_items_source_key = project.work_items_sources[0].key
        response = client.execute("""
                    mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                    updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                success
                                errorMessage
                            }
                        }
                """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert not result['success']
        assert result['errorMessage'] == f"Could not find project with key {project_key}"

    def it_checks_if_work_items_source_belongs_to_project(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = str(uuid.uuid4())
        response = client.execute("""
                    mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                    updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                success
                                errorMessage
                            }
                        }
                        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert not result['success']
        assert result[
                   'errorMessage'] == f"Work Items Source with key {work_items_source_key} does not belong to project"

    def it_checks_if_work_items_source_states_have_duplicates(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        response = client.execute("""
                    mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                    updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                success
                                errorMessage
                            }
                        }
                        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="todo", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert not result['success']
        assert result['errorMessage'] == "Invalid state map: duplicate states in the input"

    def it_validates_that_state_types_have_legal_values(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        response = client.execute("""
                            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                        success
                                        errorMessage
                                    }
                                }
                        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType="opened"),

                        ]
                    )
                ]
            )
        )
                                  )
        assert 'errors' in response
        message = response['errors'][0]['message']
        assert 'got invalid value' in message

    def it_validates_duplicate_closed_state(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        response = client.execute("""
            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                        success
                    }
                }
        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="created", stateType=WorkItemsStateType.backlog.value),
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.closed.value),
                            dict(state="closed", stateType=WorkItemsStateType.closed.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert not result['success']


@pytest.yield_fixture()
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

@pytest.yield_fixture()
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
        dict (
            seq_no= 0,
            created_at=get_date("2020-03-20"),
            state='created',
            previous_state=None
        ),
        dict(
            seq_no=1,
            created_at=get_date("2020-03-21"),
            state='done',
            previous_state='created'
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

        project.work_items_sources[0].init_state_map(
        [
            dict(state='created', state_type=WorkItemsStateType.open.value),
            dict(state='doing', state_type=WorkItemsStateType.wip.value),
            dict(state='done', state_type=WorkItemsStateType.closed.value)
        ]
    )
        project.work_items_sources[0].work_items.extend([
            WorkItem(**item)
            for item in new_work_items
        ])

        project.work_items_sources[0].work_items[0].delivery_cycles.extend([
            WorkItemDeliveryCycles(**cycle)
            for cycle in delivery_cycles
        ])

        project.work_items_sources[0].work_items[0].state_transitions.extend([
            WorkItemStateTransition(**transition)
            for transition in work_items_state_transitions
        ])

    yield project


class TestUpdateComputedWorkItemsStateTypes:

    def it_updates_computed_state_types_when_a_state_map_is_initialized(self, setup_work_items):
        client = Client(schema)
        project = setup_work_items
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_key = project.work_items_sources[0].work_items[0].key
        response = client.execute("""
            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                        success
                    }
                }
        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value),
                            dict(state="accepted", stateType=WorkItemsStateType.closed.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']

        work_items_result = db.connection().execute(f"select state, state_type from analytics.work_items "
                                f"inner join analytics.work_items_sources "
                                f"on work_items.work_items_source_id = work_items_sources.id "
                                f"where work_items_sources.key = '{work_items_source_key}'").fetchall()

        assert len(work_items_result) == 3
        assert set([
            (work_item.state, work_item.state_type)
            for work_item in work_items_result
        ]) == {
            ('todo', WorkItemsStateType.open.value),
            ('doing', WorkItemsStateType.wip.value),
            ('done', WorkItemsStateType.complete.value)
        }

    def it_updates_computed_state_types_after_initial_update(self, setup_work_items):
        client = Client(schema)
        project = setup_work_items
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_key = project.work_items_sources[0].work_items[0].key
        response = client.execute("""
            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                        success
                    }
                }
        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            # making a nonsensical assignment here to make sure the update takes
                            dict(state="todo", stateType=WorkItemsStateType.wip.value),
                            dict(state="doing", stateType=WorkItemsStateType.complete.value),
                            dict(state="done", stateType=WorkItemsStateType.open.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']

        work_items_result = db.connection().execute(f"select state, state_type from analytics.work_items "
                                f"inner join analytics.work_items_sources "
                                f"on work_items.work_items_source_id = work_items_sources.id "
                                f"where work_items_sources.key = '{work_items_source_key}'").fetchall()

        assert len(work_items_result) == 3
        assert set([
            (work_item.state, work_item.state_type)
            for work_item in work_items_result
        ]) == {
            ('todo', WorkItemsStateType.wip.value),
            ('doing', WorkItemsStateType.complete.value),
            ('done', WorkItemsStateType.open.value)
        }

    def it_resets_unmapped_entries_to_null(self, setup_work_items):
        client = Client(schema)
        project = setup_work_items
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_key = project.work_items_sources[0].work_items[0].key
        response = client.execute("""
            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                        success
                    }
                }
        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            # Leaving out the doing state from the mapping
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']

        work_items_result = db.connection().execute(f"select state, state_type from analytics.work_items "
                                                    f"inner join analytics.work_items_sources "
                                                    f"on work_items.work_items_source_id = work_items_sources.id "
                                                    f"where work_items_sources.key = '{work_items_source_key}'").fetchall()

        assert len(work_items_result) == 3
        assert set([
            (work_item.state, work_item.state_type)
            for work_item in work_items_result
        ]) == {
                   ('todo', WorkItemsStateType.open.value),
                   ('doing', None),
                   ('done', WorkItemsStateType.complete.value)
               }


class TestUpdateDeliveryCycles:

    def it_updates_delivery_cycles_when_closed_state_mapping_changes(self, work_items_delivery_cycles_setup):
        # example case to tests:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        response = client.execute("""
                    mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                    updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                success
                            }
                        }
                """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="created", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value),
                            dict(state="accepted", stateType=WorkItemsStateType.closed.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").scalar() == 1

    def it_does_not_update_delivery_cycle_when_closed_state_mapping_is_unchanged(self, work_items_delivery_cycles_setup):
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        response = client.execute("""
                            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                        success
                                    }
                                }
                        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="created", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.closed.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']

        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
             where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").scalar() == 0

    def it_is_idempotent(self, work_items_delivery_cycles_setup):
        # call twice with same inputs. Query both times to find same results in delivery cycle table
        # just to ensure there are no attempts to create duplicate entries or updates on lead time or end_date
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        response = client.execute("""
                            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                        success
                                    }
                                }
                        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="created", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value),
                            dict(state="accepted", stateType=WorkItemsStateType.closed.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                             where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").scalar() == 1


class TestUpdateDeliveryCycleDurations:

    def it_recomputes_delivery_cycle_durations_when_closed_state_type_mapping_changes(self):
        pass