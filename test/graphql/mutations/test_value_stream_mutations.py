# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from test.fixtures.graphql import *

from polaris.utils.collections import Fixture

from graphene.test import Client
from polaris.analytics.service.graphql import schema

from polaris.common import db

class TestValueStreamMutations(WorkItemApiImportTest):

    class TestCreateValueStream:

        def test_it_works(self, setup):
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