# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import pytest
from test.fixtures.graphql import *

from polaris.utils.collections import Fixture

from graphene.test import Client
from polaris.analytics.service.graphql import schema

from polaris.common import db

class TestValueStreamMutations(WorkItemApiImportTest):

    class TestCreateValueStream:

        def test_create_value_stream(self, setup):
            fixture = setup
            project=fixture.project

            client = Client(schema)

            mutation = """
                mutation createValueStream($createValueStreamInput: CreateValueStreamInput!) {
                  createValueStream(createValueStreamInput: $createValueStreamInput) {
                      success
                      valueStream {
                        id
                        name
                        key
                      }
                    }
                }
            """

            result = client.execute(mutation, variable_values=dict(
                createValueStreamInput=dict(
                    projectKey=project.key,
                    name='Application',
                    description='User facing app',
                    workItemSelectors=['ux', 'dashboard']
                )
            ))
            assert not result.get('errors')
            assert result['data']['createValueStream']['success']
            assert result['data']['createValueStream']['valueStream']['key'] is not None

    class TestEditValueStream:

        @pytest.fixture
        def setup(self, setup):
            fixture = setup
            project = fixture.project
            client = Client(schema)
            create_value_stream = """
                                    mutation createValueStream($createValueStreamInput: CreateValueStreamInput!) {
                                      createValueStream(createValueStreamInput: $createValueStreamInput) {
                                          success
                                          valueStream {
                                            id
                                            name
                                            key
                                          }
                                        }
                                    }
                                    """
            result = client.execute(create_value_stream, variable_values=dict(
                createValueStreamInput=dict(
                    projectKey=project.key,
                    name='Application',
                    description='User facing app',
                    workItemSelectors=['ux', 'dashboard']
                )
            ))
            assert not result.get('errors')
            yield Fixture(
                project = project,
                value_stream_key= result['data']['createValueStream']['valueStream']['key']
            )


        def test_edit_value_stream(self, setup):
            fixture = setup
            assert fixture.value_stream_key

            client = Client(schema)

            create_value_stream = """
                                    mutation editValueStream($editValueStreamInput: EditValueStreamInput!) {
                                      editValueStream(editValueStreamInput: $editValueStreamInput) {
                                          success
                                          valueStream {
                                            id
                                            name
                                            key
                                            description
                                            workItemSelectors
                                          }
                                        }
                                    }
                                """

            result = client.execute(create_value_stream, variable_values=dict(
                editValueStreamInput=dict(
                    projectKey=fixture.project.key,
                    valueStreamKey=fixture.value_stream_key,
                    name='Application2',
                )
            ))
            assert not result.get('errors')
            assert result['data']['editValueStream']['success']
            assert result['data']['editValueStream']['valueStream']['key'] is not None
            assert result['data']['editValueStream']['valueStream']['name'] == 'Application2'
            assert result['data']['editValueStream']['valueStream']['description'] == 'User facing app'
            assert result['data']['editValueStream']['valueStream']['workItemSelectors'] == ['ux', 'dashboard']

