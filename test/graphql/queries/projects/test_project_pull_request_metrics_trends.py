# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal
import uuid

from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from test.graphql.queries.projects.shared_fixtures import exclude_repos_from_project
from test.graphql.queries.projects.shared_testing_mixins import TrendingWindowMeasurementDate

class TestProjectPullRequestMetricsTrends:

    @pytest.fixture()
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
                end_date=None,
                **pull_requests_common
            )
            for i in range(0, 2)
        ]

        yield Fixture(
            project=project,
            api_helper=api_helper,
            start_date=start_date,
            pull_requests=pull_requests,
            repositories=repositories,
            work_items_common=work_items_common,
            organization=organization,
            work_items_source=work_items_source
        )

    class TestPullRequestMetricsTrends:
        @pytest.fixture()
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
                                measurementWindow: 1,
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
                                measurementWindow,
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

        class TestTrendingWindowContract(TrendingWindowMeasurementDate):
            @pytest.fixture
            def setup(self, setup):
                fixture = setup

                yield Fixture(
                    parent=setup,
                    output_attribute='pullRequestMetricsTrends'
                )


        class TestWhenNoPullRequests:

            def it_returns_zero_values_for_each_measurement(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key
                ))
                assert result['data']
                project = result['data']['project']
                assert len(project['pullRequestMetricsTrends']) == 31
                for measurement in project['pullRequestMetricsTrends']:
                    assert measurement['totalOpen'] == 0
                    assert measurement['totalClosed'] == 0
                    assert measurement['avgAge'] == 0
                    assert measurement['minAge'] == 0
                    assert measurement['percentileAge'] == 0

        class TestWithTwoPullRequests:

            @pytest.fixture()
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
                        assert measurement['totalOpen'] == 0
                        assert measurement['totalClosed'] == 0
                        assert measurement['avgAge'] == 0
                        assert measurement['minAge'] == 0
                        assert measurement['percentileAge'] == 0

                class TestWhenOneOpenOneClosedPullRequests:

                    @pytest.fixture()
                    def setup(self, setup):
                        fixture = setup
                        api_helper = fixture.api_helper
                        # close 1 PR at now() - it should be recognized
                        api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                       update_dict=dict(state='closed', end_date=datetime.utcnow()))
                        yield fixture

                    def it_returns_one_closed_pr_that_was_closed_just_now(self, setup):
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
                        assert int(metrics_values['avgAge']) == 10
                        assert int(metrics_values['minAge']) == 10
                        assert int(metrics_values['maxAge']) == 10
                        assert int(metrics_values['percentileAge']) == 10
                        for measurement in project['pullRequestMetricsTrends'][1:]:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['percentileAge'] == 0


                    def it_returns_two_closed_prs_that_were_closed_just_now(self, setup):
                        fixture = setup
                        client = Client(schema)

                        # close 2nd PR
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[1]['key'],
                                                       update_dict=dict(state='closed',
                                                                        end_date=datetime.utcnow()))

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        metrics_values = project['pullRequestMetricsTrends'][0]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 2
                        assert int(metrics_values['avgAge']) == 10
                        assert int(metrics_values['minAge']) == 10
                        assert int(metrics_values['maxAge']) == 10
                        assert int(metrics_values['percentileAge']) == 10
                        for measurement in project['pullRequestMetricsTrends'][1:]:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['maxAge'] == 0
                            assert measurement['percentileAge'] == 0

                    def it_returns_two_closed_prs_that_were_closed_in_two_different_periods(self, setup):
                        fixture = setup
                        client = Client(schema)

                        # close 2nd PR
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[1]['key'],
                                                       update_dict=dict(state='closed',
                                                                        end_date=datetime.utcnow() - timedelta(days=1)))

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        metrics_values = project['pullRequestMetricsTrends'][0]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 1
                        assert int(metrics_values['avgAge']) == 10
                        assert int(metrics_values['minAge']) == 10
                        assert int(metrics_values['maxAge']) == 10
                        assert int(metrics_values['percentileAge']) == 10

                        metrics_values = project['pullRequestMetricsTrends'][1]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 1
                        assert int(metrics_values['avgAge']) == 9
                        assert int(metrics_values['minAge']) == 9
                        assert int(metrics_values['maxAge']) == 9
                        assert int(metrics_values['percentileAge']) == 9

                        for measurement in project['pullRequestMetricsTrends'][2:]:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['maxAge'] == 0
                            assert measurement['percentileAge'] == 0

        class TestSpecsOnlyFlag:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Import and map 2 PRs to work item
                api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])

                work_items_api_helper = WorkItemImportApiHelper(fixture.organization, fixture.work_items_source)
                work_items = [
                    dict(
                        key=uuid.uuid4().hex,
                        name=f'Issue',
                        display_id='1000',
                        state='backlog',
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                        **fixture.work_items_common
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name=f'Issue',
                        display_id='10001',
                        state='backlog',
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                        **fixture.work_items_common
                    )
                ]
                work_items_api_helper.import_work_items(work_items)


                fixture.query = """
                query getProjectPullRequestMetricsTrends(
                    $project_key:String!,
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [PullRequestMetricsTrends], 
                            pullRequestMetricsTrendsArgs: {
                                measurementWindow: 1,
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
                                ],
                                specsOnly: true
                            }
                        ) {
                            pullRequestMetricsTrends {
                                measurementDate,
                                measurementWindow,
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
                    work_items=work_items,
                    repository=fixture.repositories['alpha']
                )

            class TestWhenTwoOpenNoClosedPullRequests:

                def it_returns_null_values_when_there_are_no_closed_pull_requests_that_are_specs(self, setup):
                    fixture = setup
                    client = Client(schema)

                    result = client.execute(fixture.query, variable_values=dict(
                        project_key=fixture.project.key
                    ))

                    assert result['data']
                    project = result['data']['project']
                    assert len(project['pullRequestMetricsTrends']) == 31
                    for measurement in project['pullRequestMetricsTrends']:
                        assert measurement['totalOpen'] == 0
                        assert measurement['totalClosed'] == 0
                        assert measurement['avgAge'] == 0
                        assert measurement['minAge'] == 0
                        assert measurement['percentileAge'] == 0

                class TestWhenOneOpenOneClosedPullRequests:

                    def it_only_returns_one_closed_pr_that_was_closed_just_now_and_is_a_spec(self, setup):
                        fixture = setup
                        client = Client(schema)
                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'], fixture.pull_requests[0]['key'])
                        # close 1 PR at now() - it should be recognized
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                       update_dict=dict(state='closed', end_date=datetime.utcnow()))

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        metrics_values = project['pullRequestMetricsTrends'][0]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 1
                        assert int(metrics_values['avgAge']) == 10
                        assert int(metrics_values['minAge']) == 10
                        assert int(metrics_values['maxAge']) == 10
                        assert int(metrics_values['percentileAge']) == 10
                        for measurement in project['pullRequestMetricsTrends'][1:]:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['percentileAge'] == 0

                    def it_excludes_one_closed_pr_that_was_closed_just_now_if_it_is_not_a_spec(self, setup):
                        fixture = setup
                        client = Client(schema)

                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'],
                                                                         fixture.pull_requests[0]['key'])
                        # close 1 PR at now() - it should not be recognized since its not a spec.
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[1]['key'],
                                                       update_dict=dict(state='closed', end_date=datetime.utcnow()))

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        for measurement in project['pullRequestMetricsTrends']:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['percentileAge'] == 0



                    def it_reports_only_unique_pull_request_when_a_single_pr_is_mapped_to_multiple_work_items(self, setup):
                        fixture = setup
                        client = Client(schema)
                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'], fixture.pull_requests[0]['key'])
                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[1]['key'],
                                                                         fixture.pull_requests[0]['key'])
                        # close 1 PR at now() - it should be recognized
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                       update_dict=dict(state='closed', end_date=datetime.utcnow()))

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        metrics_values = project['pullRequestMetricsTrends'][0]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 1
                        assert int(metrics_values['avgAge']) == 10
                        assert int(metrics_values['minAge']) == 10
                        assert int(metrics_values['maxAge']) == 10
                        assert int(metrics_values['percentileAge']) == 10
                        for measurement in project['pullRequestMetricsTrends'][1:]:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['percentileAge'] == 0

                    def it_reports_only_closed_pull_requests_when_a_single_work_item__is_mapped_to_multiple_prs(self, setup):
                        fixture = setup
                        client = Client(schema)
                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'], fixture.pull_requests[0]['key'])
                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'],
                                                                         fixture.pull_requests[1]['key'])

                        # close 1 PR at now() - it should be recognized, the other PR is not closed so it should be ignored.
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                       update_dict=dict(state='closed', end_date=datetime.utcnow()))

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        metrics_values = project['pullRequestMetricsTrends'][0]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 1
                        assert int(metrics_values['avgAge']) == 10
                        assert int(metrics_values['minAge']) == 10
                        assert int(metrics_values['maxAge']) == 10
                        assert int(metrics_values['percentileAge']) == 10
                        for measurement in project['pullRequestMetricsTrends'][1:]:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['percentileAge'] == 0

                    def it_reports_all_closed_pull_requests_when_a_single_work_item__is_mapped_to_multiple_prs(self,
                                                                                                                setup):
                        fixture = setup
                        client = Client(schema)
                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'],
                                                                         fixture.pull_requests[0]['key'])
                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'],
                                                                         fixture.pull_requests[1]['key'])

                        # close 1 PR at now() - it should be recognized,
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                               update_dict=dict(state='closed',
                                                                                end_date=datetime.utcnow()))

                        # close the second PR at now() - it should be recognized.
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[1]['key'],
                                                               update_dict=dict(state='closed',
                                                                                end_date=datetime.utcnow()))

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        metrics_values = project['pullRequestMetricsTrends'][0]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 2
                        assert int(metrics_values['avgAge']) == 10
                        assert int(metrics_values['minAge']) == 10
                        assert int(metrics_values['maxAge']) == 10
                        assert int(metrics_values['percentileAge']) == 10
                        for measurement in project['pullRequestMetricsTrends'][1:]:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['percentileAge'] == 0

                    def it_excludes_repositories_that_are_excluded_from_the_project(self, setup):
                        fixture = setup
                        client = Client(schema)
                        fixture.api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'], fixture.pull_requests[0]['key'])
                        # close 1 PR at now() - it should be recognized
                        fixture.api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'],
                                                       update_dict=dict(state='closed', end_date=datetime.utcnow()))

                        exclude_repos_from_project(fixture.project, [fixture.repository])

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']
                        assert len(project['pullRequestMetricsTrends']) == 31
                        metrics_values = project['pullRequestMetricsTrends'][0]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 0
                        assert int(metrics_values['avgAge']) == 0
                        assert int(metrics_values['minAge']) == 0
                        assert int(metrics_values['maxAge']) == 0
                        assert int(metrics_values['percentileAge']) == 0
                        for measurement in project['pullRequestMetricsTrends'][1:]:
                            assert measurement['totalOpen'] == 0
                            assert measurement['totalClosed'] == 0
                            assert measurement['avgAge'] == 0
                            assert measurement['minAge'] == 0
                            assert measurement['percentileAge'] == 0

