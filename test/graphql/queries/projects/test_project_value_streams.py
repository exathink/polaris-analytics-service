# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client
from test.fixtures.graphql import *
from test.constants import *
from polaris.analytics.service.graphql import schema
from polaris.analytics.db.model import ValueStream

class TestProjectValueStreams(OrgRepoTest):

    class TestConnection:
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
                project=project_mercury,
                value_streams=Fixture(
                    enhancements=enhancements,
                    production_defects=production_defects
                )
            )

        def it_returns_the_value_streams_for_a_project(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                            query getProjectValueStreams($key: String!){
                                project(key: $key) {
                                    valueStreams {
                                        count
                                    }
                                }
                            }
                        """

            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert not result.get('errors')
            assert result['data']
            assert result['data']['project']['valueStreams']['count'] == 2

        def it_returns_implicit_interfaces_for_value_streams(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                            query getProjectValueStreams($key: String!){
                                project(key: $key) {
                                    valueStreams {
                                        count
                                        edges {    
                                            node {
                                                id
                                                name
                                                key
                                                workItemSelectors
                                            }
                                        }
                                    }
                                }
                            }
                        """

            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert not result.get('errors')
            assert result['data']
            value_streams = {
                (edge['node']['key'], edge['node']['name'], tuple(edge['node']['workItemSelectors']))
                for edge in
                result['data']['project']['valueStreams']['edges']
            }
            assert value_streams == {
                (str(fixture.value_streams.production_defects.key), 'Production Defects', ('production_bug',)),
                (str(fixture.value_streams.enhancements.key), 'Product Enhancements', ('enhancements', 'features'))
            }



