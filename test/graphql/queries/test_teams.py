# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.teams import *

from graphene.test import Client
from polaris.analytics.service.graphql import schema

class TestTeamNode:

    @pytest.yield_fixture
    def setup(self, setup_teams):
        fixture = setup_teams

        yield fixture


    def it_resolves_team_nodes(self, setup):
        fixture = setup

        query = """
                query getTeam($key: String!) {
                    team(key: $key){
                        id
                        name
                        key
                    }
                }
                """
        client = Client(schema)
        result = client.execute(query, variable_values=dict(key=fixture.team_a['key']))
        assert result['data']
        assert result['data']['team']['key'] == str(fixture.team_a['key'])


class TestOrganizationTeams:

    @pytest.yield_fixture
    def setup(self, setup_teams):
        fixture = setup_teams

        yield fixture

    def it_resolves_teams_for_an_organization(self, setup):
        fixture = setup

        query = """
                        query getOrganizationTeams($key: String!) {
                            organization(key: $key){
                                teams {
                                    count
                                    edges {
                                        node {
                                            id
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                        """
        client = Client(schema)
        result = client.execute(query, variable_values=dict(key=fixture.organization.key))
        assert result['data']
        assert result['data']['organization']['teams']['count'] == 2