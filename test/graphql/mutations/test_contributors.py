# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from test.fixtures.contributors import *
from test.fixtures.teams import *

from graphene.test import Client
from unittest.mock import patch

from polaris.analytics.db.model import Contributor

from polaris.analytics.service.graphql import schema
from polaris.messaging.test_utils import assert_topic_and_message
from polaris.analytics.messaging.messages import ContributorTeamAssignmentsChanged
from polaris.messaging.topics import AnalyticsTopic

class TestUpdateContributorForContributorAlias:

    def it_returns_success_when_contributor_aliases_are_updated(self, setup_commits_for_contributor_updates):
        client = Client(schema)
        query = """
            mutation updateAlias($contributorInfo: ContributorInfo! ){
                updateContributor(
                    contributorInfo: $contributorInfo
                ){
                    updateStatus 
                    {
                        contributorKey
                        success
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(
            contributorInfo=dict(
                contributorKey=joe_contributor_key,
                updatedInfo=dict(
                    contributorAliasKeys=[joe_alt_contributor_key]
                )
            )
        ))
        assert 'errors' not in result
        assert result['data']['updateContributor']['updateStatus']['success']

    def it_sets_a_contributor_to_be_excluded_from_analysis(self, setup_commits_for_contributor_updates):
        client = Client(schema)
        query = """
                    mutation updateAlias($contributorInfo: ContributorInfo! ){
                        updateContributor(
                            contributorInfo: $contributorInfo
                        ){
                            updateStatus 
                            {
                                contributorKey
                                success
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            contributorInfo=dict(
                contributorKey=joe_contributor_key,
                updatedInfo=dict(
                    excludedFromAnalysis=True
                )
            )
        ))
        assert 'errors' not in result
        assert result['data']['updateContributor']['updateStatus']['success']
        # Check using db query if the contributor is excluded from analysis
        db.connection().execute(
            f"select count(repository_id) from analytics.repositories_contributor_aliases "
            f"join analytics.contributor_aliases on repositories_contributor_aliases.contributor_alias_id=contributor_aliases.id "
            f"where contributor_aliases.key='{joe_contributor_key}' "
            f"and contributor_aliases.robot=true "
            f"and repositories_contributor_aliases.robot=true").scalar() == 1

    def it_returns_failure_message_when_contributor_not_found(self, setup_commits_for_contributor_updates):
        test_contributor_key =uuid.uuid4()
        client = Client(schema)
        query = """
                    mutation updateAlias($contributorInfo: ContributorInfo! ){
                        updateContributor(
                            contributorInfo: $contributorInfo
                        ){
                            updateStatus 
                            {
                                contributorKey
                                success
                                message
                                exception
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            contributorInfo=dict(
                contributorKey=test_contributor_key,
                updatedInfo=dict(
                    contributorAliasKeys=[joe_alt_contributor_key]
                )
            )
        ))
        assert 'errors' not in result
        assert not result['data']['updateContributor']['updateStatus']['success']
        assert result['data']['updateContributor']['updateStatus']['exception'] == f"Contributor with key: {test_contributor_key} was not found"

    def it_returns_failure_message_when_contributor_alias_not_found(self, setup_commits_for_contributor_updates):
        test_contributor_key = uuid.uuid4()
        client = Client(schema)
        query = """
                            mutation updateAlias($contributorInfo: ContributorInfo! ){
                                updateContributor(
                                    contributorInfo: $contributorInfo
                                ){
                                    updateStatus 
                                    {
                                        contributorKey
                                        success
                                        message
                                        exception
                                    }
                                }
                            }
                        """
        result = client.execute(query, variable_values=dict(
            contributorInfo=dict(
                contributorKey=joe_contributor_key,
                updatedInfo=dict(
                    contributorAliasKeys=[test_contributor_key]
                )
            )
        ))
        assert 'errors' not in result
        assert not result['data']['updateContributor']['updateStatus']['success']
        assert result['data']['updateContributor']['updateStatus']['exception'] == f"Could not find contributor alias with key {test_contributor_key}"


class TestUpdateContributorTeamAssignments:

    @pytest.fixture
    def setup(self, setup_team_assignments):
        yield setup_team_assignments


    def it_updates_team_assignments(self, setup):
        fixture = setup

        client = Client(schema)
        query = """
                mutation updateAssignments($input: UpdateContributorTeamAssignmentsInput! ){
                    updateContributorTeamAssignments(
                        updateContributorTeamAssignmentsInput: $input
                    ){
                        updateCount
                        success
                        errorMessage
                        
                    }
                    
                }
                """
        with patch('polaris.analytics.publish.publish'):
            result = client.execute(query, variable_values=dict(
                input=dict(
                    organizationKey=fixture.organization.key,
                    contributorTeamAssignments=[
                        dict(
                            contributorKey=str(fixture.joe.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.alice.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.arjun.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                    ]
                )
            ))
            assert 'errors' not in result
            assert result['data']['updateContributorTeamAssignments']['success']

    def it_assigns_the_new_team_correctly(self, setup):
        fixture = setup

        client = Client(schema)
        query = """
                mutation updateAssignments($input: UpdateContributorTeamAssignmentsInput! ){
                    updateContributorTeamAssignments(
                        updateContributorTeamAssignmentsInput: $input
                    ){
                        updateCount
                        success
                        errorMessage

                    }

                }
                """

        with patch('polaris.analytics.publish.publish'):
            result = client.execute(query, variable_values=dict(
                input=dict(
                    organizationKey=fixture.organization.key,
                    contributorTeamAssignments=[
                        dict(
                            contributorKey=str(fixture.joe.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.alice.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.arjun.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                    ]
                )
            ))
            assert db.connection().execute(
                "select count(distinct team_id) from analytics.contributors_teams where end_date is null").scalar() == 1



    def it_sets_the_end_date_on_the_old_assignments(self, setup):
        fixture = setup

        client = Client(schema)
        query = """
                mutation updateAssignments($input: UpdateContributorTeamAssignmentsInput! ){
                    updateContributorTeamAssignments(
                        updateContributorTeamAssignmentsInput: $input
                    ){
                        updateCount
                        success
                        errorMessage

                    }

                }
                """

        with patch('polaris.analytics.publish.publish'):
            result = client.execute(query, variable_values=dict(
                input=dict(
                    organizationKey=fixture.organization.key,
                    contributorTeamAssignments=[
                        dict(
                            contributorKey=str(fixture.joe.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.alice.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.arjun.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                    ]
                )
            ))
            assert db.connection().execute("select count(id) from analytics.contributors_teams where end_date is not null").scalar() == 3

    def it_sets_the_default_capacity_on_all_assignments(self, setup):
        fixture = setup

        client = Client(schema)
        query = """
                mutation updateAssignments($input: UpdateContributorTeamAssignmentsInput! ){
                    updateContributorTeamAssignments(
                        updateContributorTeamAssignmentsInput: $input
                    ){
                        updateCount
                        success
                        errorMessage

                    }

                }
                """
        with patch('polaris.analytics.publish.publish'):
            result = client.execute(query, variable_values=dict(
                input=dict(
                    organizationKey=fixture.organization.key,
                    contributorTeamAssignments=[
                        dict(
                            contributorKey=str(fixture.joe.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.alice.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.arjun.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                    ]
                )
            ))
            assert db.connection().execute("select count(id) from analytics.contributors_teams where capacity=1").scalar() == 6

    def it_publishes_the_contributor_team_assignment_changed_message_on_success(self, setup):
        fixture = setup

        client = Client(schema)
        query = """
                mutation updateAssignments($input: UpdateContributorTeamAssignmentsInput! ){
                    updateContributorTeamAssignments(
                        updateContributorTeamAssignmentsInput: $input
                    ){
                        updateCount
                        success
                        errorMessage

                    }

                }
                """

        with patch('polaris.analytics.publish.contributor_team_assignments_changed') as publish:
            input_assignments = [
                dict(
                    contributorKey=str(fixture.joe.key),
                    newTeamKey=str(fixture.team_c['key'])
                ),
                dict(
                    contributorKey=str(fixture.alice.key),
                    newTeamKey=str(fixture.team_c['key'])
                ),
                dict(
                    contributorKey=str(fixture.arjun.key),
                    newTeamKey=str(fixture.team_c['key'])
                ),
            ]
            result = client.execute(query, variable_values=dict(
                input=dict(
                    organizationKey=fixture.organization.key,
                    contributorTeamAssignments=input_assignments
                )
            ))
            publish.assert_called()
            assert len(publish.call_args[0]) == 2
            assert publish.call_args[0][0] == str(fixture.organization.key)
            assert len(publish.call_args[0][1]) == 3
            for assignment in publish.call_args[0][1]:
                assert not assignment['initial_assignment']

    def it_does_not_publishes_the_contributor_team_assignment_changed_message_on_failure(self, setup):
        fixture = setup

        client = Client(schema)
        query = """
                mutation updateAssignments($input: UpdateContributorTeamAssignmentsInput! ){
                    updateContributorTeamAssignments(
                        updateContributorTeamAssignmentsInput: $input
                    ){
                        updateCount
                        success
                        errorMessage

                    }

                }
                """

        with patch('polaris.analytics.publish.contributor_team_assignments_changed') as publish:
            input_assignments = [
                dict(
                    # make a random contributor key raise an error
                    contributorKey=str(uuid.uuid4()),
                    newTeamKey=str(fixture.team_c['key'])
                ),
            ]
            result = client.execute(query, variable_values=dict(
                input=dict(
                    organizationKey=fixture.organization.key,
                    contributorTeamAssignments=input_assignments
                )
            ))
            assert not result['data']['updateContributorTeamAssignments']['success']
            publish.assert_not_called()

    def it_publishes_to_the_analytics_service_topic(self, setup):
        fixture = setup

        client = Client(schema)
        query = """
                mutation updateAssignments($input: UpdateContributorTeamAssignmentsInput! ){
                    updateContributorTeamAssignments(
                        updateContributorTeamAssignmentsInput: $input
                    ){
                        updateCount
                        success
                        errorMessage

                    }

                }
                """

        with patch('polaris.analytics.publish.publish') as publish:
            result = client.execute(query, variable_values=dict(
                input=dict(
                    organizationKey=fixture.organization.key,
                    contributorTeamAssignments=[
                        dict(
                            contributorKey=str(fixture.joe.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.alice.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                        dict(
                            contributorKey=str(fixture.arjun.key),
                            newTeamKey=str(fixture.team_c['key'])
                        ),
                    ]
                )
            ))
        assert_topic_and_message(publish, AnalyticsTopic, ContributorTeamAssignmentsChanged)


    class TestInitialAssignmentDetection:

        @pytest.fixture
        def setup(self, setup_teams):
            # In this case we are setting thing up without any initial assignments
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
                session.add_all([joe, alice, arjun])

            yield Fixture(
                parent=fixture,
                joe=joe,
                alice=alice,
                arjun=arjun
            )

        def it_detects_initial_assignments_correctly(self, setup):
            fixture = setup

            client = Client(schema)
            query = """
                    mutation updateAssignments($input: UpdateContributorTeamAssignmentsInput! ){
                        updateContributorTeamAssignments(
                            updateContributorTeamAssignmentsInput: $input
                        ){
                            updateCount
                            success
                            errorMessage

                        }

                    }
                    """

            with patch('polaris.analytics.publish.contributor_team_assignments_changed') as publish:
                input_assignments = [
                    dict(
                        contributorKey=str(fixture.joe.key),
                        newTeamKey=str(fixture.team_c['key'])
                    ),
                    dict(
                        contributorKey=str(fixture.alice.key),
                        newTeamKey=str(fixture.team_c['key'])
                    ),
                    dict(
                        contributorKey=str(fixture.arjun.key),
                        newTeamKey=str(fixture.team_c['key'])
                    ),
                ]
                result = client.execute(query, variable_values=dict(
                    input=dict(
                        organizationKey=fixture.organization.key,
                        contributorTeamAssignments=input_assignments
                    )
                ))
                publish.assert_called()
                assert len(publish.call_args[0]) == 2
                assert publish.call_args[0][0] == str(fixture.organization.key)
                assert len(publish.call_args[0][1]) == 3
                for assignment in publish.call_args[0][1]:
                    assert assignment['initial_assignment']