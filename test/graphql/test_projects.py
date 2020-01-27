# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
import uuid
from polaris.common import db
from polaris.analytics.db.model import Project
from graphene.test import Client
from polaris.analytics.service.graphql import schema

from test.fixtures.repo_org import *

test_projects = [
    dict(name='mercury', key=uuid.uuid4()),
    dict(name='venus', key=uuid.uuid4())
]

@pytest.fixture()
def setup_projects(setup_org):
    organization = setup_org
    for project in test_projects:
        with db.orm_session() as session:
            session.add(organization)
            organization.projects.append(
                Project(
                    name=project['name'],
                    key=project['key']
                )
            )



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
    def it_creates_a_github_source_state_map(self, setup_schema):
        client = Client(schema)

        response = client.execute("""
            mutation updateProjectStateMaps {
                    updateProjectStateMaps(
                        updateProjectStateMapsInput:{
                            projectKey:"2b10652d-d0c2-4059-a178-060610daef62",
                            workItemsSourceStateMaps: [
                            {
                                workItemsSourceKey:"afa3c667-f7d5-4806-8af1-94b531c03dc5", 
                                stateMaps:[
                                    {
                                        state: "todo",
                                        stateType: "open"
                                    },
                                    {
                                        state: "doing",
                                        stateType:"wip",
                                    },
                                    {
                                        state:"done",
                                        stateType:"complete"
                                    }
                                ]
                            }
                        ]
                    }){
                        success
                    }
                }
        """)
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']