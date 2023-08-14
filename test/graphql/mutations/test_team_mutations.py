# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from test.fixtures.teams import *
from test.fixtures.graphql import OrgRepoTest
from polaris.utils.collections import Fixture

from graphene.test import Client
from polaris.analytics.service.graphql import schema

from polaris.common import db
from polaris.analytics.db.model import Team


class TestCreateTeam(OrgRepoTest):

    class TestCreate:
        @pytest.fixture()
        def setup(self, setup, cleanup_teams):
            fixture = setup
            organization = fixture.organization

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


class TestUpdateTeamSettings(OrgRepoTest):
    class TestUpdateSettings:
        @pytest.fixture()
        def setup(self, setup, cleanup_teams):
            fixture = setup
            organization = fixture.organization
            team_key = uuid.uuid4()

            with db.orm_session() as session:
                session.add(organization)
                organization.teams.append(
                    Team(
                        key=team_key,
                        name="test team",
                        settings=dict(
                            flow_metrics_settings=dict(
                                lead_time_target=30,
                                cycle_time_target=14,
                                include_sub_tasks=False
                            ),
                            analysis_periods=dict(
                                wip_analysis_period=7,
                                flow_analysis_period=14,
                                trend_analysis_period=30
                            )
                        )
                    )
                )

            mutation = """
                            mutation updateTeamSettings($updateTeamSettingsInput: UpdateTeamSettingsInput!) {
                                updateTeamSettings(updateTeamSettingsInput: $updateTeamSettingsInput) {
                                    success
                                    errorMessage
                                }
                            }

            """

            yield Fixture(
                parent=fixture,
                team_key=team_key,
                mutation=mutation
            )

        def it_updates_the_team_settings(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.mutation, variable_values=dict(
                updateTeamSettingsInput=dict(
                    key=str(fixture.team_key),
                    flowMetricsSettings=dict(
                        leadTimeTarget=14,
                        cycleTimeTarget=7,
                        includeSubTasks=False
                    )
                )
            ))
            assert not result.get('errors')
            update = result['data']['updateTeamSettings']
            assert update['success']

            team = db.connection().execute(
                f"select settings from analytics.teams where key='{fixture.team_key}'",
                dict(key=str(fixture.team_key))
            ).fetchone()
            assert team['settings']['flow_metrics_settings'] == dict(
                lead_time_target=14,
                cycle_time_target=7,
                include_sub_tasks=False
            )

        def it_updates_the_team_name_if_passed(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.mutation, variable_values=dict(
                updateTeamSettingsInput=dict(
                    key=str(fixture.team_key),
                    name='new name'
                )
            ))
            assert not result.get('errors')
            update = result['data']['updateTeamSettings']
            assert update['success']

            assert db.connection().execute(
                f"select name from analytics.teams where key='{fixture.team_key}'",
                dict(key=str(fixture.team_key))
            ).scalar() == 'new name'

        def it_updates_the_team_work_item_selectors_if_passed(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.mutation, variable_values=dict(
                updateTeamSettingsInput=dict(
                    key=str(fixture.team_key),
                    workItemSelectors=['team:Buffalo']
                )
            ))
            assert not result.get('errors')
            update = result['data']['updateTeamSettings']
            assert update['success']

            assert db.connection().execute(
                f"select work_item_selectors from analytics.teams where key='{fixture.team_key}'",
                dict(key=str(fixture.team_key))
            ).fetchone()[0] == ['team:Buffalo']


        def it_leaves_the_other_settings_alone(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.mutation, variable_values=dict(
                updateTeamSettingsInput=dict(
                    key=str(fixture.team_key),
                    flowMetricsSettings=dict(
                        leadTimeTarget=14,
                        cycleTimeTarget=7,
                        includeSubTasks=False
                    )
                )
            ))
            assert not result.get('errors')
            update = result['data']['updateTeamSettings']
            assert update['success']

            team = db.connection().execute(
                f"select settings from analytics.teams where key='{fixture.team_key}'",
                dict(key=str(fixture.team_key))
            ).fetchone()
            assert team['settings']['analysis_periods'] == dict(
                wip_analysis_period=7,
                flow_analysis_period=14,
                trend_analysis_period=30
            )

        def it_leaves_everything_alone_if_not_passed(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.mutation, variable_values=dict(
                updateTeamSettingsInput=dict(
                    key=str(fixture.team_key),
                )
            ))
            assert not result.get('errors')
            update = result['data']['updateTeamSettings']
            assert update['success']

            team = db.connection().execute(
                f"select name, settings from analytics.teams where key='{fixture.team_key}'",
                dict(key=str(fixture.team_key))
            ).fetchone()
            assert team['name'] == 'test team'
            assert team['settings'] == dict(
                flow_metrics_settings=dict(
                    lead_time_target=30,
                    cycle_time_target=14,
                    include_sub_tasks=False
                ),
                analysis_periods=dict(
                    wip_analysis_period=7,
                    flow_analysis_period=14,
                    trend_analysis_period=30
                )
            )
