# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from test.fixtures.teams import *

from graphene.test import Client
from polaris.analytics.service.graphql import schema
from polaris.common import db
from polaris.utils.collections import Fixture
from polaris.analytics.db.model import Contributor, ContributorTeam, Team


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
        assert result['data']['organization']['teams']['count'] == 3


class TestContributorTeams:

    @pytest.yield_fixture
    def setup(self, setup_team_assignments):
        yield setup_team_assignments



    def it_resolves_current_teams_for_contributors(self, setup):
        fixture  = setup
        query = """
                query getContributorTeam($key: String!) {
                    contributor(key: $key, interfaces:[TeamNodeRef]){
                        id,
                        name,
                        key, 
                        teamName, 
                        teamKey
                    }
                }
                """
        client = Client(schema)
        result = client.execute(query, variable_values=dict(key=fixture.joe.key))
        assert result['data']
        assert result['data']['contributor']['teamName'] == fixture.team_a['name']

    def it_resolves_contributor_count_for_teams(self, setup):
        fixture = setup
        query = """
                query getTeamContributorCount($key: String!) {
                    organization(key: $key){
                        teams(interfaces : [ContributorCount]) {
                            edges {
                                node {
                                    id
                                    name
                                    key
                                    contributorCount
                                }
                            }
                        }
                    }
                }
                   """
        client = Client(schema)
        result = client.execute(query, variable_values=dict(key=fixture.organization.key))
        assert result['data']
        assert {
            (edge['node']['name'], edge['node']['contributorCount'])
                for edge in result['data']['organization']['teams']['edges']
        } == {
            ('Team Alpha', 1),
            ('Team Beta', 2),
            ('Team Delta', 0)
        }