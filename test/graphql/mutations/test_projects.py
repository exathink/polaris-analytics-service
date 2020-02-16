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
from polaris.analytics.db.model import Project, WorkItemsSource, WorkItem
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


@pytest.yield_fixture()
def setup_work_items(setup_projects):
    organization = setup_projects
    project = organization.projects[0]
    # open state_type of this new work item should be updated to complete after the test
    new_work_items = [
        dict(
            key=uuid.uuid4().hex,
            name='Issue 10',
            display_id='1000',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            is_bug=True,
            work_item_type='issue',
            url='http://foo.com',
            tags=['ares2'],
            state='done',
            description='foo',
            source_id=str(uuid.uuid4()),
            state_type='open'
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
    def it_updates_project_work_item_state(self, setup_work_items):
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

        query = """
            query getWorkItem($key:String!) {
                workItem(key: $key, interfaces: [WorkItemInfo]){
                    description
                    displayId
                    state
                    stateType
                }
            } 
        """
        work_item_result = client.execute(query, variable_values=dict(key=work_item_key))
        assert 'data' in work_item_result
        assert work_item_result['data']['workItem']['stateType'] == WorkItemsStateType.complete.value

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
        assert not result
        assert 'errors' in response
        message = response['errors'][0]['message']
        assert message=="Could not find project with key: %s" %project_key


    def it_checks_if_work_items_source_belongs_to_project(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = str(uuid.uuid4())
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
                            dict(state="done", stateType=WorkItemsStateType.complete.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert not result
        assert 'errors' in response
        message = response['errors'][0]['message']
        assert message == "Work item source with key: %s is not associated to project with key: %s" % (work_items_source_key, project_key)

    def it_checks_if_work_items_source_states_have_duplicates(self, setup_work_items_sources):
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
        assert not result
        assert 'errors' in response
        message = response['errors'][0]['message']
        assert message == "Invalid state map: duplicate states in the input"

    def it_validates_that_state_types_have_legal_values(self, setup_work_items_sources):
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