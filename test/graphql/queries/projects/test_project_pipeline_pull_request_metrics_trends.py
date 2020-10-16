# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestProjectPipelinePullRequestMetricsTrends:

    @pytest.yield_fixture()
    def setup(self, api_pull_requests_import_fixture):
        organization, project, repositories, work_items_source, work_items_common, pull_requests_common = api_pull_requests_import_fixture
        api_helper = PullRequestImportApiHelper(organization, repositories, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
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
            work_items=work_items,
            pull_requests=pull_requests,
            repositories=repositories
        )


    class TestPullRequestMetrics:
        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup
            metrics_query = """
                query getProjectPullRequestMetrics(
                    $project_key:String!,
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [PipelinePullRequestMetrics], 
                            pipelinePullRequestMetricsArgs: {
                                metrics: [
                                    total_open
                                    total_closed
                                    avg_age
                                ]
                            }
                        ) {
                            pipelinePullRequestMetrics {
                                totalOpen
                                totalClosed
                                avgAge
                            }
                        }
                }
            """

            yield Fixture(
                parent=fixture,
                query=metrics_query,
                output_attribute='pipelinePullRequestMetrics'
            )

        class TestWhenNoWorkItems:
            # FIXME: Fix Query API to return zero values in such a case
            def it_returns_zero_value_for_each_metric(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key
                ))

                assert result['data']
                project = result['data']['project']
                assert len(project[fixture.output_attribute]) == 3

        class TestWhenWorkItemIsOpen:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                api_helper.import_work_items(fixture.work_items)

                yield fixture

            class TestWhenNoPullRequests:
                # FIXME: Fix Query API to return zero values in such a case
                def it_returns_zero_value_for_each_metric(self, setup):
                    fixture = setup

                    client = Client(schema)

                    result = client.execute(fixture.query, variable_values=dict(
                        project_key=fixture.project.key
                    ))

                    assert result['data']
                    project = result['data']['project']
                    assert len(project[fixture.output_attribute]) == 3

            class TestWithTwoPullRequests:

                @pytest.yield_fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper
                    # Import and map 2 PRs to work item
                    api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])
                    for pr in fixture.pull_requests:
                        api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'], pr['key'])

                    yield fixture

                class TestWhenTwoOpenNoClosedPullRequests:
                    def it_returns_correct_metrics(self, setup):
                        fixture = setup
                        client = Client(schema)

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']

                        metrics_values = project[fixture.output_attribute]
                        assert metrics_values['totalOpen'] == 2
                        assert metrics_values['totalClosed'] == 0
                        assert metrics_values['avgAge'] > 0


                class TestWhenNoOpenTwoClosedPullRequests:

                    @pytest.yield_fixture()
                    def setup(self, setup):
                        fixture = setup

                        api_helper = fixture.api_helper
                        # close both PRs
                        for pr in fixture.pull_requests:
                            api_helper.update_pull_request(pull_request_key=pr['key'], update_dict=dict(state='closed'))
                        yield fixture

                    def it_returns_zero_total_open_two_total_closed_prs(self, setup):
                        fixture = setup

                        client = Client(schema)

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']

                        metrics_values = project[fixture.output_attribute]
                        assert metrics_values['totalOpen'] == 0
                        assert metrics_values['totalClosed'] == 2
                        assert metrics_values['avgAge'] > 0

                class TestWhenOneOpenOneClosedPullRequests:

                    @pytest.yield_fixture()
                    def setup(self, setup):
                        fixture = setup
                        api_helper = fixture.api_helper
                        # close 1 mapped PR
                        api_helper.update_pull_request(pull_request_key=fixture.pull_requests[0]['key'], update_dict=dict(state='closed'))
                        yield fixture

                    def it_returns_one_open_one_closed_pr(self, setup):
                        fixture = setup
                        client = Client(schema)

                        result = client.execute(fixture.query, variable_values=dict(
                            project_key=fixture.project.key
                        ))

                        assert result['data']
                        project = result['data']['project']

                        metrics_values = project[fixture.output_attribute]
                        assert metrics_values['totalOpen'] == 1
                        assert metrics_values['totalClosed'] == 1
                        assert metrics_values['avgAge'] > 0

            class TestWhenDeliveryCycleIsClosed:
                # TODO: Add this case to Query API
                @pytest.yield_fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper
                    api_helper.update_delivery_cycles(
                        updates=[
                            (0, dict(property='end_date', value=fixture.start_date + timedelta(days=2)))
                        ]
                    )

                    yield fixture

                def it_returns_two_open_prs(self, setup):
                    fixture = setup
                    client = Client(schema)

                    result = client.execute(fixture.query, variable_values=dict(
                        project_key=fixture.project.key
                    ))

                    assert result['data']
                    project = result['data']['project']

                    metrics_values = project[fixture.output_attribute]
                    assert metrics_values['totalOpen'] == 2
                    assert metrics_values['totalClosed'] == 0
                    assert metrics_values['avgAge'] > 0

        class TestWhenWorkItemIsClosed:
            class TestWhenDeliveryCycleIsOpen:
                class TestWhenTwoOpenNoClosedPullRequests:
                    pass

                class TestWhenNoOpenPullRequests:
                    pass

                class TestWhenOneOpenOneClosedPullRequests:
                    pass

            class TesWhenDeliveryCycleIsClosed:
                class TestWhenTwoOpenNoClosedPullRequests:
                    pass

                class TestWhenNoOpenPullRequests:
                    pass

                class TestWhenOneOpenOneClosedPullRequests:
                    pass



