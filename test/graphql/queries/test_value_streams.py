# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2023) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import pytest
from graphene.test import Client
from test.fixtures.graphql import *
from test.constants import *
from polaris.analytics.service.graphql import schema
from polaris.analytics.db.model import ValueStream
class TestValueStreams(OrgRepoTest):

    class TestNamedNode:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup
            enhancements = ValueStream(
                name='Product Enhancements',
                key=uuid.uuid4(),
                work_item_selectors=['enhancements', 'features']
            )
            production_defects = ValueStream(
                name='Production Defects',
                key=uuid.uuid4(),
                work_item_selectors=['production_bug']
            )
            project_mercury = fixture.projects['mercury']

            with db.orm_session() as session:
                session.add(project_mercury)
                project_mercury.value_streams.extend([
                    enhancements,
                    production_defects
                ])

            yield Fixture(
                parent=fixture,
                value_streams=Fixture(
                    enhancements=enhancements,
                    production_defects=production_defects
                )
            )



        def it_fetches_a_value_stream(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                query getValueStream($key: String!){
                    valueStream(key: $key) {
                        id
                        name
                        key
                    }
                }
            """

            result = client.execute(query, variable_values=dict(
                key=fixture.value_streams.enhancements.key
            ))

            assert result['data']
            assert not result.get('errors')
            assert result['data']['valueStream']

        def it_supports_the_value_stream_info_interface(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                query getValueStream($key: String!){
                    valueStream(key: $key) {
                        id
                        name
                        key
                        workItemSelectors 
                    }
                }
            """

            result = client.execute(query, variable_values=dict(
                key=fixture.value_streams.enhancements.key
            ))

            assert result['data']
            assert not result.get('errors')
            assert len(result['data']['valueStream']['workItemSelectors']) == 2
