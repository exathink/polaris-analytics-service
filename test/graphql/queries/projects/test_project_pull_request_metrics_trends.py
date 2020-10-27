# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestProjectPullRequestMetricsTrends:

    @pytest.yield_fixture()
    def setup(self, api_pull_requests_import_fixture):
        organization, project, repositories, work_items_source, work_items_common, pull_requests_common = api_pull_requests_import_fixture
        api_helper = PullRequestImportApiHelper(organization, repositories, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        pull_requests = [
            dict(
                repository_id=repositories['alpha'].id,
                key=uuid.uuid4().hex,
                source_id=f'100{i}',
                source_branch='1000',
                source_repository_id=repositories['alpha'].id,
                title="Another change. Fixes issue #1000",
                created_at=start_date,
                updated_at=start_date,
                merged_at=None,
                **pull_requests_common
            )
            for i in range(0, 2)
        ]

        yield Fixture(
            project=project,
            api_helper=api_helper,
            start_date=start_date,
            pull_requests=pull_requests,
            repositories=repositories
        )

    class TestPullRequestMetricsTrends:
        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup
            trends_query = """
                query getProjectPullRequestMetricsTrends(
                    $project_key:String!,
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [PullRequestMetricsTrends], 
                            pullRequestMetricsTrendsArgs: {
                                measurementWindow: 7,
                                days: 30,
                                samplingFrequency: 1,
                                pullRequestAgeTargetPercentile: 0.9,
                                metrics: [
                                    total_open
                                    total_closed
                                    avg_age
                                    min_age
                                    max_age
                                    percentile_age
                                ]
                            }
                        ) {
                            pullRequestMetricsTrends {
                                measurementDate,
                                totalOpen
                                totalClosed
                                avgAge
                                minAge
                                maxAge
                                percentileAge
                            }
                        }
                }
            """

            yield Fixture(
                parent=fixture,
                query=trends_query
            )

        class TestWhenNoPullRequests:

            def it_returns_null_values_for_each_measurement(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key
                ))
                assert result['data']
                project = result['data']['project']
                assert len(project['pullRequestMetricsTrends']) == 31
                for measurement in project['pullRequestMetricsTrends']:
                    assert measurement['totalOpen'] == None
                    assert measurement['totalClosed'] == None
                    assert measurement['avgAge'] == None
                    assert measurement['minAge'] == None
                    assert measurement['percentileAge'] == None

        class TestWithTwoPullRequests:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Import and map 2 PRs to work item
                api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])
                # for pr in fixture.pull_requests:
                #     api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'], pr['key'])

                yield fixture

            class TestWhenTwoOpenNoClosedPullRequests:

                def it_returns_null_values_for_closed_pull_requests(self, setup):
                    fixture = setup
                    client = Client(schema)

                    result = client.execute(fixture.query, variable_values=dict(
                        project_key=fixture.project.key
                    ))

                    assert result['data']
                    project = result['data']['project']
                    assert len(project['pullRequestMetricsTrends']) == 31
                    for measurement in project['pullRequestMetricsTrends']:
                        assert measurement['totalOpen'] == None
                        assert measurement['totalClosed'] == None
                        assert measurement['avgAge'] == None
                        assert measurement['minAge'] == None
                        assert measurement['percentileAge'] == None

                class TestWhenOneOpenOneClosedPullRequests:

                    @pytest.yield_fixture()
                    def setup(self, setup):
                        fixture = setup
                        api_helper = fixture.api_helper
                        # close 1 PR
                        api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                       update_dict=dict(state='closed', updated_at=datetime.utcnow()-timedelta(days=1)))
                        yield fixture

                    def it_returns_one_closed_pr_only_for_latest_time_window(self, setup):
                        fixture = setup
                        client = Client(schema)

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        metrics_values = project['pullRequestMetricsTrends'][0]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 1
                        assert int(metrics_values['avgAge']) == 9
                        assert int(metrics_values['minAge']) == 9
                        assert int(metrics_values['maxAge']) == 9
                        assert int(metrics_values['percentileAge']) == 9
                        for measurement in project['pullRequestMetricsTrends'][1:]:
                            assert measurement['totalOpen'] == None
                            assert measurement['totalClosed'] == None
                            assert measurement['avgAge'] == None
                            assert measurement['minAge'] == None
                            assert measurement['percentileAge'] == None

                    class TestWhenTwoClosedPullRequests:

                        @pytest.yield_fixture()
                        def setup(self, setup):
                            fixture = setup
                            api_helper = fixture.api_helper
                            # close 2nd PR
                            api_helper.update_pull_request(pull_request_key=fixture.pull_requests[1]['key'],
                                                           update_dict=dict(state='closed',
                                                                            updated_at=datetime.utcnow() - timedelta(
                                                                                days=1)))
                            yield fixture

                        def it_returns_two_closed_prs_only_for_latest_time_window(self, setup):
                            fixture = setup
                            client = Client(schema)

                            result = client.execute(fixture.query, variable_values=dict(
                                project_key=fixture.project.key
                            ))

                            assert result['data']
                            project = result['data']['project']
                            assert len(project['pullRequestMetricsTrends']) == 31
                            metrics_values = project['pullRequestMetricsTrends'][0]
                            assert metrics_values['totalOpen'] == 0
                            assert metrics_values['totalClosed'] == 2
                            assert int(metrics_values['avgAge']) == 9
                            assert int(metrics_values['minAge']) == 9
                            assert int(metrics_values['maxAge']) == 9
                            assert int(metrics_values['percentileAge']) == 9
                            for measurement in project['pullRequestMetricsTrends'][1:]:
                                assert measurement['totalOpen'] == None
                                assert measurement['totalClosed'] == None
                                assert measurement['avgAge'] == None
                                assert measurement['minAge'] == None
                                assert measurement['maxAge'] == None
                                assert measurement['percentileAge'] == None