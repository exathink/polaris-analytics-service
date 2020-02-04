# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
import uuid
from polaris.common import db
from polaris.analytics.db.model import Project, WorkItemsSource
from graphene.test import Client
from polaris.analytics.service.graphql import schema
from polaris.utils.collections import find
from polaris.analytics.db.enums import WorkItemsStateType

from test.fixtures.repo_org import *
from test.constants import *

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
                                 dict(state="todo", stateType=WorkItemsStateType.open),
                                 dict(state="doing", stateType=WorkItemsStateType.wip),
                                 dict(state="done", stateType=WorkItemsStateType.complete)
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
                            dict(state="todo", stateType=WorkItemsStateType.open),
                            dict(state="doing", stateType=WorkItemsStateType.wip),
                            dict(state="done", stateType=WorkItemsStateType.complete)
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
                            dict(state="todo", stateType=WorkItemsStateType.open),
                            dict(state="doing", stateType=WorkItemsStateType.wip),
                            dict(state="done", stateType=WorkItemsStateType.complete)
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
                            dict(state="todo", stateType=WorkItemsStateType.open),
                            dict(state="todo", stateType=WorkItemsStateType.wip),
                            dict(state="done", stateType=WorkItemsStateType.complete)
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