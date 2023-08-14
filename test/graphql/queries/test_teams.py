# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from test.fixtures.teams import *

from datetime import datetime, timedelta
from graphene.test import Client
from polaris.analytics.service.graphql import schema
from polaris.common import db
from polaris.utils.collections import Fixture, dict_merge
from polaris.analytics.db.model import Contributor, ContributorTeam, Team
from test.fixtures.graphql import api_pull_requests_import_fixture, PullRequestImportApiHelper, WorkItemApiImportTest, api_work_items_import_fixture
from test.fixtures.teams import *
class TestTeamNode:

    @pytest.fixture
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

class TestTeamWorkItems(WorkItemApiImportTest):
    class TestWorkItems:

        def update_team_selectors(self, team, selectors):
            with db.orm_session() as session:
                team = Team.find_by_key(session, team['key'])
                team.work_item_selectors.extend(selectors)

        @pytest.fixture
        def setup(self, setup, setup_teams):
            fixture = Fixture.merge(setup, setup_teams)

            yield Fixture(
                parent=fixture
            )



        def it_returns_the_inferred_work_items(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items_common = dict_merge(
                fixture.work_items_common,
                dict(created_at=datetime.utcnow(), updated_at=datetime.utcnow())
            )

            work_items = [
                dict(
                    key=uuid.uuid4(),
                    name=f'Issue {i}',
                    display_id='1000',
                    state='open',
                    **work_items_common
                )
                for i in range(0, 5)
            ]
            api_helper.import_work_items(work_items)
            map_work_items_to_team(fixture.team_a, work_items[0:3])

            query = """
                    query getTeamWorkItems($key: String!) {
                        team(key: $key) {
                            workItems {
                                count
                                edges {
                                    node {
                                        key
                                    }
                                }
                            }
                        }
                    }
                    """

            client = Client(schema)
            result = client.execute(query, variable_values=dict(key=fixture.team_a['key']))
            assert result['data']['team']['workItems']['count'] == 3
            assert set([edge['node']['key'] for edge in result['data']['team']['workItems']['edges']]) == set([str(wi['key']) for wi in work_items[0:3]])

        def it_returns_the_work_items_based_on_selectors(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items_common = dict_merge(
                fixture.work_items_common,
                dict(created_at=datetime.utcnow(), updated_at=datetime.utcnow())
            )


            # The tags on these work items identify the teams they belong to
            work_items_with_selectors = [
                dict(
                    key=uuid.uuid4(),
                    name=f'Issue S A 1',
                    display_id='S-A 1',
                    state='open',
                    **dict_merge(work_items_common, dict(tags=['team:Team_A']))
                ),
                dict(
                    key=uuid.uuid4(),
                    name=f'Issue S A - 2',
                    display_id='S-A-2',
                    state='open',
                    **dict_merge(work_items_common, dict(tags=['team:Team_A']))
                ),
                dict(
                    key=uuid.uuid4(),
                    name=f'Issue S A - 2',
                    display_id='S-A-2',
                    state='open',
                    **dict_merge(work_items_common, dict(tags=[]))
                ),
            ]
            api_helper.import_work_items(work_items_with_selectors)
            # The team needs to specify the selectors for the team
            self.update_team_selectors(fixture.team_a, ['team:Team_A'])

            query = """
                           query getTeamWorkItems($key: String!) {
                               team(key: $key) {
                                   workItems {
                                       count
                                       edges {
                                           node {
                                               key
                                           }
                                       }
                                   }
                               }
                           }
                           """

            client = Client(schema)
            result = client.execute(query, variable_values=dict(key=fixture.team_a['key']))
            assert result['data']['team']['workItems']['count'] == 2
            assert set([edge['node']['key'] for edge in result['data']['team']['workItems']['edges']]) == set(
                [str(wi['key']) for wi in work_items[0:2]])
class TestTeamPullRequests:
    @pytest.fixture
    def setup_pull_requests_fixture(self, api_pull_requests_import_fixture):
        organization, project, repositories, work_items_source, work_items_common, pull_requests_common = api_pull_requests_import_fixture
        api_helper = PullRequestImportApiHelper(organization, repositories, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4(),
                name=f'Issue {i}',
                display_id='1000',
                state='open',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 1)
        ]

        pull_requests = [
            dict(
                repository_id=repositories['alpha'].id,
                key=uuid.uuid4(),
                source_id=f'100{i}',
                source_branch='1000',
                source_repository_id=repositories['alpha'].id,
                title="Another change. Fixes issue #1000",
                created_at=start_date,
                updated_at=start_date,
                end_date=None,
                **pull_requests_common
            )
            for i in range(0, 2)
        ]

        yield Fixture(
            project=project,
            api_helper=api_helper,
            start_date=start_date,
            work_items=work_items,
            pull_requests=pull_requests,
            repositories=repositories
        )

    @pytest.fixture
    def setup(self, setup_teams, setup_pull_requests_fixture):

        teams_fixture=setup_teams
        pull_requests_fixture=setup_pull_requests_fixture

        yield Fixture(
            parent=setup_teams,
            pull_requests_fixture=setup_pull_requests_fixture
        )

    class TestAllPullRequestsFromTeamRepos:
        @pytest.fixture
        def setup(self, setup):
            fixture = setup
            pull_requests_fixture = fixture.pull_requests_fixture

            # add repository alpha to team_a
            with db.orm_session() as session:
                team_a = Team.find_by_key(session, fixture.team_a['key'])
                team_a.repositories.append(pull_requests_fixture.repositories['alpha'])

            # import some pull requests into the repository alpha
            api_helper = pull_requests_fixture.api_helper
            api_helper.import_pull_requests(pull_requests_fixture.pull_requests,
                                            pull_requests_fixture.repositories['alpha'])

            yield fixture

        def it_returns_all_team_pull_requests(self, setup):
            fixture = setup

            query = """
                query getTeamPullRequests($key: String!) {
                    team(key: $key) {
                        pullRequests {
                            count
                        }
                    }
                }
            
            """

            client = Client(schema)
            result = client.execute(query, variable_values=dict(key=fixture.team_a['key']))
            assert result['data']['team']['pullRequests']['count'] == 2

        def it_returns_all_team_pull_requests_when_specs_is_false(self, setup):
            fixture = setup

            query = """
                query getTeamPullRequests($key: String!) {
                    team(key: $key) {
                        pullRequests(specsOnly: false) {
                            count
                        }
                    }
                }

            """

            client = Client(schema)
            result = client.execute(query, variable_values=dict(key=fixture.team_a['key']))
            assert result['data']['team']['pullRequests']['count'] == 2

        def it_does_not_return_any_pull_requests_when_specs_is_true_and_there_are_no_specs(self, setup):
            fixture = setup

            query = """
                query getTeamPullRequests($key: String!) {
                    team(key: $key) {
                        pullRequests(specsOnly: true) {
                            count
                        }
                    }
                }

            """

            client = Client(schema)
            result = client.execute(query, variable_values=dict(key=fixture.team_a['key']))
            assert result['data']['team']['pullRequests']['count'] == 0

        class TestPullRequestsSpecsOnly:

            @pytest.fixture
            def setup(self, setup):
                fixture = setup
                pull_requests_fixture = fixture.pull_requests_fixture
                api_helper = pull_requests_fixture.api_helper


                # Import work items
                api_helper.import_work_items(pull_requests_fixture.work_items)
                yield fixture


            def it_does_not_return_pull_requests_when_work_items_are_not_mapped_to_pull_requests_but_not_to_team(self, setup):
                fixture = setup
                pull_requests_fixture = fixture.pull_requests_fixture
                api_helper = pull_requests_fixture.api_helper

                # Map work items to pull request 1
                for work_item in pull_requests_fixture.work_items:
                    api_helper.map_pull_request_to_work_item(work_item['key'],
                                                             pull_requests_fixture.pull_requests[0]['key'])

                query = """
                                query getTeamPullRequests($key: String!) {
                                    team(key: $key) {
                                        pullRequests(specsOnly: true) {
                                            count
                                        }
                                    }
                                }

                            """

                client = Client(schema)
                result = client.execute(query, variable_values=dict(key=fixture.team_a['key']))
                assert result['data']['team']['pullRequests']['count'] == 0

            def it_returns_pull_requests_when_work_items_are_mapped_to_pull_requests_and_to_team(self, setup):
                fixture = setup
                pull_requests_fixture = fixture.pull_requests_fixture
                api_helper = pull_requests_fixture.api_helper

                # Map work items to pull request 1
                for work_item in pull_requests_fixture.work_items:
                    api_helper.map_pull_request_to_work_item(work_item['key'],
                                                             pull_requests_fixture.pull_requests[0]['key'])
                # map work_item 0 to team_a
                map_work_items_to_team(fixture.team_a, [pull_requests_fixture.work_items[0]])

                query = """
                                query getTeamPullRequests($key: String!) {
                                    team(key: $key) {
                                        pullRequests(specsOnly: true) {
                                            count
                                        }
                                    }
                                }

                            """

                client = Client(schema)
                result = client.execute(query, variable_values=dict(key=fixture.team_a['key']))
                assert result['data']['team']['pullRequests']['count'] == 1

class TestOrganizationTeams:

    @pytest.fixture
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

    @pytest.fixture
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