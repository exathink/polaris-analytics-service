# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from test.fixtures.teams import *

from polaris.utils.collections import Fixture

from graphene.test import Client
from polaris.analytics.service.graphql import schema

from polaris.common import db

class TestCreateTeam:

    @pytest.fixture()
    def setup(self, org_repo_fixture, cleanup_teams):
        organization, projects, repositories = org_repo_fixture

        mutation = """
            mutation createTeam($organizationKey: String!, $name: String!) {
                createTeam(createTeamInput: {
                    organizationKey: $organizationKey,
                    name: $name
                }) {
                    team {
                        id
                        name
                        key
                    }
                    success
                    errorMessage
                }
            }
        """

        yield Fixture(
            organization=organization,
            mutation=mutation
        )

    def it_creates_a_team_in_an_organization(self, setup):
        fixture = setup

        client = Client(schema)
        result = client.execute(
            fixture.mutation,
            variable_values=dict(
                organizationKey=fixture.organization.key,
                name="Team Buffalo"
            )
        )
        assert 'errors' not in result
        assert result['data']['createTeam']['success']

    def it_returns_the_key_of_the_created_team(self, setup):
        fixture = setup

        client = Client(schema)
        result = client.execute(
            fixture.mutation,
            variable_values=dict(
                organizationKey=fixture.organization.key,
                name="Team Buffalo"
            )
        )
        assert 'errors' not in result
        assert result['data']['createTeam']['team']['key'] is not None

    def it_creates_the_team_in_the_database(self, setup):
        fixture = setup

        client = Client(schema)
        result = client.execute(
            fixture.mutation,
            variable_values=dict(
                organizationKey=fixture.organization.key,
                name="Team Buffalo"
            )
        )
        assert 'errors' not in result
        key = result['data']['createTeam']['team']['key']

        assert db.connection().execute(f"select id from analytics.teams where key='{key}'").scalar() is not None


    def it_raises_an_error_if_the_organization_is_not_valid(self, setup):
        fixture = setup

        client = Client(schema)
        result = client.execute(
            fixture.mutation,
            variable_values=dict(
                organizationKey=str(uuid.uuid4()),
                name="Team Buffalo"
            )
        )
        assert 'errors' not in result
        assert not result['data']['createTeam']['success']
        assert result['data']['createTeam']['errorMessage']