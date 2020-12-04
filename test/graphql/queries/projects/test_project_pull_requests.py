# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from graphene.test import Client

from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestProjectPullRequests:

    @pytest.yield_fixture()
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

    class TestProjectPullRequestsConnection:
        class TestWithNoPullRequest:

            @pytest.yield_fixture()
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

            class TestWithActivePullRequests:

                @pytest.yield_fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper
                    # Import 2 PRs
                    api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])

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
                    @pytest.yield_fixture()
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

    class TestProjectPullRequestEventSpan:

        @pytest.yield_fixture()
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
