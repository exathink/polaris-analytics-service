# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from graphene.test import Client

from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from polaris.analytics.service.graphql.utils import get_before_date

class TestProjectPullRequests:

    @pytest.fixture()
    def setup(self, api_pull_requests_import_fixture):
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

        work_items_api_helper = WorkItemImportApiHelper(organization, work_items_source)
        work_items_api_helper.import_work_items(work_items)

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
            work_items_api_helper=work_items_api_helper,
            start_date=start_date,
            work_items=work_items,
            pull_requests=pull_requests,
            repositories=repositories
        )

    class TestProjectPullRequestsConnection:
        class TestWithNoPullRequest:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup
                query = """
                    query getProjectPullRequests($key:String!) {
                        project(key: $key){
                            pullRequests {
                                edges {
                                    node {
                                        id
                                        name
                                        key
                                        age
                                        state
                                        createdAt
                                        endDate
                                    }
                                }
                            }
                        }
                     }
                """
                yield Fixture(
                    parent=fixture,
                    query=query
                )

            def it_returns_empty_list(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    key=fixture.project.key
                ))

                assert result['data']
                assert result['data']['project']['pullRequests']['edges'] == []

            class TestWithActivePullRequestsFromProject:

                @pytest.fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper

                    # Import 2 PRs
                    api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])
                    fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'],
                                                                     fixture.pull_requests[0]['key'])
                    fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'],
                                                                     fixture.pull_requests[1]['key'])

                    yield fixture

                def it_returns_all_pull_requests(self, setup):
                    fixture = setup

                    client = Client(schema)

                    result = client.execute(fixture.query, variable_values=dict(
                        key=fixture.project.key
                    ))

                    assert result['data']
                    assert len(result['data']['project']['pullRequests']['edges']) == 2

                def it_returns_all_prs_with_active_only_false(self, setup):
                    fixture = setup

                    client = Client(schema)
                    query = """
                    query getProjectPullRequests($key:String!) {
                        project(key: $key){
                            pullRequests (activeOnly: false) {
                                edges {
                                    node {
                                        id
                                        name
                                        key
                                        age
                                        state
                                        createdAt
                                        endDate
                                    }
                                }
                            }
                        }
                     }
                """

                    result = client.execute(fixture.query, variable_values=dict(
                        key=fixture.project.key
                    ))

                    assert result['data']
                    assert len(result['data']['project']['pullRequests']['edges']) == 2

                class TestWithOneClosedPullRequest:
                    @pytest.fixture()
                    def setup(self, setup):
                        fixture = setup
                        api_helper = fixture.api_helper
                        # Close 1 PR
                        api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                       update_dict=dict(state='closed', end_date=datetime.utcnow()))
                        yield fixture

                    def it_returns_one_pr_with_active_only_true(self, setup):
                        fixture = setup

                        client = Client(schema)
                        query = """
                        query getProjectPullRequests($key:String!) {
                            project(key: $key){
                                pullRequests (activeOnly: true) {
                                    edges {
                                        node {
                                            id
                                            name
                                            key
                                            age
                                            state
                                            createdAt
                                            endDate
                                        }
                                    }
                                }
                            }
                         }
                        """

                        result = client.execute(query, variable_values=dict(
                            key=fixture.project.key
                        ))

                        assert result['data']
                        assert len(result['data']['project']['pullRequests']['edges']) == 1
                        assert result['data']['project']['pullRequests']['edges'][0]['node']['key'] == str(
                            fixture.pull_requests[1]['key'])

                    def it_returns_pr_closed_within_1_day(self, setup):
                        fixture = setup

                        client = Client(schema)
                        query = """
                                            query getProjectPullRequests($key:String!) {
                                                project(key: $key){
                                                    pullRequests (closedWithinDays: 1) {
                                                        edges {
                                                            node {
                                                                id
                                                                name
                                                                key
                                                                age
                                                                state
                                                                createdAt
                                                                endDate
                                                            }
                                                        }
                                                    }
                                                }
                                             }
                                            """

                        result = client.execute(query, variable_values=dict(
                            key=fixture.project.key
                        ))

                        assert result['data']
                        assert len(result['data']['project']['pullRequests']['edges']) == 1

                    class TestBeforeDateHandling:

                        def it_returns_items_closed_within_the_window_starting_at_the_end_of_the_before_date_when_it_is_a_date_time(self, setup):
                            fixture = setup
                            api_helper = fixture.api_helper
                            # Set the target before date to be a point in the middle of a day
                            before = datetime.utcnow()
                            # the expected behavior is to start the search in a window starting at the end of
                            # the before date
                            end_of_today = before.date() + timedelta(days=1)
                            # so close the pull request two days before the end of today.
                            api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                           update_dict=dict(state='closed', end_date=end_of_today - timedelta(days=2)))
                            client = Client(schema)
                            query = """
                                                query getProjectPullRequests($key:String!, $before: DateTime!, $days: Int!) {
                                                    project(key: $key){
                                                        pullRequests (closedWithinDays: $days, before: $before) {
                                                            edges {
                                                                node {
                                                                    id
                                                                    name
                                                                    key
                                                                    age
                                                                    state
                                                                    createdAt
                                                                    endDate
                                                                }
                                                            }
                                                        }
                                                    }
                                                 }
                                                """

                            result = client.execute(query, variable_values=dict(
                                key=fixture.project.key,
                                days=2,
                                before=before
                            ))

                            assert result['data']
                            assert len(result['data']['project']['pullRequests']['edges']) == 1

                    def it_excludes_items_closed_within_the_window_starting_with_the_before_date_it_is_a_date_time(
                            self, setup):
                        fixture = setup
                        api_helper = fixture.api_helper
                        # Set the target before date to be a point in the middle of a day
                        before = datetime.utcnow()

                        # now close the pull request two days before the actual before date
                        # we should not find the pull request since it falls outside the window. This is counter-intuitive
                        # but the expected behavior given the conventions around the search window
                        api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                       update_dict=dict(state='closed',
                                                                        end_date=before - timedelta(days=2)))
                        client = Client(schema)
                        query = """
                                            query getProjectPullRequests($key:String!, $before: DateTime!, $days: Int!) {
                                                project(key: $key){
                                                    pullRequests (closedWithinDays: $days, before: $before) {
                                                        edges {
                                                            node {
                                                                id
                                                                name
                                                                key
                                                                age
                                                                state
                                                                createdAt
                                                                endDate
                                                            }
                                                        }
                                                    }
                                                }
                                             }
                                            """

                        result = client.execute(query, variable_values=dict(
                            key=fixture.project.key,
                            days=2,
                            before=before
                        ))

                        assert result['data']
                        assert len(result['data']['project']['pullRequests']['edges']) == 0

            class TestSpecsVsAllPullRequests:

                @pytest.fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper

                    # Import 2 PRs
                    api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])
                    # map one to work item in project
                    fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'],
                                                                     fixture.pull_requests[0]['key'])
                    fixture.query = """
                                query getProjectPullRequests($key:String!, $specsOnly: Boolean) {
                                    project(key: $key){
                                        pullRequests (specsOnly: $specsOnly) {
                                            edges {
                                                node {
                                                    id
                                                    name
                                                    key
                                                    age
                                                    state
                                                    createdAt
                                                    endDate
                                                }
                                            }
                                        }
                                    }
                                 }
                                """

                    yield fixture

                def it_returns_only_project_pull_requests_when_specs_only_is_true(self, setup):
                    fixture = setup
                    client = Client(schema)
                    result = client.execute(fixture.query, variable_values=dict(
                        key=fixture.project.key,
                        specsOnly=True
                    ))

                    assert result['data']
                    assert len(result['data']['project']['pullRequests']['edges']) == 1

                def it_returns_pull_requests_from_all_repos_in_the_project_when_specs_only_is_false(self, setup):
                    fixture = setup
                    client = Client(schema)
                    result = client.execute(fixture.query, variable_values=dict(
                        key=fixture.project.key,
                        specsOnly=False
                    ))

                    assert result['data']
                    assert len(result['data']['project']['pullRequests']['edges']) == 2

                def it_returns_pull_requests_from_all_repos_in_the_project_when_specs_only_is_not_specified(self, setup):
                    fixture = setup
                    client = Client(schema)
                    result = client.execute(fixture.query, variable_values=dict(
                        key=fixture.project.key,
                    ))

                    assert result['data']
                    assert len(result['data']['project']['pullRequests']['edges']) == 2


    class TestProjectPullRequestEventSpan:

        @pytest.fixture()
        def setup(self, setup):
            fixture = setup

            latest_pull_request_date = fixture.start_date + timedelta(days=3)
            fixture.pull_requests[0]['updated_at'] = latest_pull_request_date

            fixture.api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])

            yield Fixture(
                parent=fixture,
                latest_pull_request_date=latest_pull_request_date
            )

        def it_returns_the_latest_pull_request_date(self, setup):
            fixture = setup

            query = """
                        query getProjectPullRequests($key:String!) {
                            project(key: $key, interfaces: [PullRequestEventSpan]){
                                latestPullRequestEvent
                            }
                         }
                    """

            client = Client(schema)
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert result['data']
            assert graphql_date(
                result['data']['project']['latestPullRequestEvent']
            ) == fixture.latest_pull_request_date
