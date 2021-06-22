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
        assert result['data']['organization']['teams']['count'] == 2


class TestContributorTeams:

    @pytest.yield_fixture
    def setup(self, setup_teams):
        fixture = setup_teams

        with db.orm_session() as session:
            joe = Contributor(
                name='Joe',
                key=uuid.uuid4()
            )
            alice = Contributor(
                name="Alice",
                key=uuid.uuid4()
            )
            arjun = Contributor(
                name='Arjun',
                key=uuid.uuid4()
            )
            team_a = fixture.team_a['key']
            team_b = fixture.team_b['key']

            session.add(joe)
            joe.assign_to_team(session, team_a)

            session.add(alice)
            alice.assign_to_team(session, team_b)

            session.add(arjun)
            arjun.assign_to_team(session, team_b)


        yield Fixture(
            parent=fixture,
            joe=joe,
            alice=alice,
            arjun=arjun
        )

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