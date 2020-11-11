# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta

from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from test.fixtures.work_items_commits import *


class TestPullRequestInstance:

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

    class TestNamedNodeInterface:

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup
            query = """
                query getPullRequest($key:String!) {
                            pullRequest(key: $key, interfaces:[NamedNode]){
                                id
                                name
                                key
                                age
                                state
                                createdAt
                                endDate
                            }
                        } 
            """
            yield Fixture(
                parent=fixture,
                query=query
            )

        def it_returns_null_when_pull_request_with_given_key_does_not_exist(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query, variable_values=dict(
                key=fixture.pull_requests[0]['key']
            ))
            assert result['data']
            assert not result['data']['pullRequest']

        class TestWhenPullRequestExists:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Import 2 PRs
                api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])

                yield fixture

            def it_returns_correct_pull_request_details(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    key=fixture.pull_requests[0]['key']
                ))

                assert result['data']
                pull_request = result['data']['pullRequest']
                assert pull_request['key'] == str(fixture.pull_requests[0]['key'])
                assert pull_request['state'] == 'open'
                assert pull_request['createdAt'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                assert int(pull_request['age']) == 10
                assert pull_request['endDate'] == None

    class TestBranchRefInterface:

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup
            query = """
                        query getPullRequest($key:String!) {
                                    pullRequest(key: $key, interfaces:[NamedNode, BranchRef]){
                                        id
                                        name
                                        key
                                        age
                                        state
                                        createdAt
                                        endDate
                                        repositoryKey
                                        repositoryName
                                        branchName
                                    }
                                } 
                    """
            yield Fixture(
                parent=fixture,
                query=query
            )

        def it_returns_null_when_pull_request_with_given_key_does_not_exist(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query, variable_values=dict(
                key=fixture.pull_requests[0]['key']
            ))
            assert result['data']
            assert not result['data']['pullRequest']

        class TestWhenPullRequestExists:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Import 2 PRs
                api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])

                yield fixture

            def it_returns_correct_pull_request_details(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    key=fixture.pull_requests[0]['key']
                ))

                assert result['data']
                pull_request = result['data']['pullRequest']
                assert pull_request['key'] == str(fixture.pull_requests[0]['key'])
                assert pull_request['repositoryKey'] == str(uuid.UUID(fixture.repositories['alpha'].key))
                assert pull_request['repositoryName'] == 'alpha'
                assert pull_request['branchName'] == '1000'

    class TestPullRequestWorkItemsSummariesInterface:

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup
            query = """
                query getPullRequest($key:String!) {
                    pullRequest(key: $key, interfaces:[NamedNode, WorkItemsSummaries]){
                        id
                        name
                        key
                        workItemsSummaries {
                            displayId
                            name
                            key
                            url
                            workItemType
                            state
                            stateType
                        }
                    }
                } 
            """
            yield Fixture(
                parent=fixture,
                query=query
            )

        class TestWithNoWorkItems:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Import PRs
                api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])

                yield fixture

            def it_returns_no_work_items_summaries(self, setup):
                fixture = setup
                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(
                    key=fixture.pull_requests[0]['key']
                ))
                assert result['data']
                assert len(result['data']['pullRequest']["workItemsSummaries"]) == 0

            class TestWithLinkedWorkItems:

                @pytest.yield_fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper
                    # Import work items
                    api_helper.import_work_items(fixture.work_items)
                    # Map work items to pull request 1
                    for work_item in fixture.work_items:
                        api_helper.map_pull_request_to_work_item(work_item['key'], fixture.pull_requests[0]['key'])

                    yield fixture

                def it_returns_correct_work_items_summaries(self, setup):
                    fixture = setup
                    client = Client(schema)

                    result = client.execute(fixture.query, variable_values=dict(
                        key=fixture.pull_requests[0]['key']
                    ))
                    assert result['data']
                    work_items_summaries = result['data']['pullRequest']["workItemsSummaries"]
                    assert len(work_items_summaries) == 1
                    for work_item in work_items_summaries:
                        assert work_item["displayId"]
                        assert work_item["name"]
                        assert work_item["key"]
                        assert work_item["url"]
                        assert work_item["workItemType"]
                        assert work_item["state"]
                        assert work_item["stateType"] == None
