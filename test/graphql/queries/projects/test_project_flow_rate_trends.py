# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from graphene.test import Client

from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from polaris.utils.collections import Fixture
from test.graphql.queries.projects.shared_testing_mixins import \
    TrendingWindowMeasurementDate


class TestProjectFlowRateTrends:

    @pytest.yield_fixture()
    def setup(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        yield Fixture(
            work_items=work_items,
            project=project,
            api_helper=api_helper,
            start_date=start_date
        )

    class TestTrendingWindows(
        TrendingWindowMeasurementDate
    ):

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup

            query = """
                    query getProjectFlowRateTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [FlowRateTrends],
                            flowRateTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [
                                    arrival_rate,
                                    close_rate
                                ],
                            }
                        )
                        {
                            flowRateTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                                closeRate
                            }
                        }
                    }
            """
            yield Fixture(
                parent=fixture,
                query=query,
                output_attribute='flowRateTrends'
            )

    class TestForAllWorkitemsInBacklog:

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup

            api_helper = fixture.api_helper
            api_helper.import_work_items(fixture.work_items)

            yield Fixture(
                parent=fixture
            )

        class TestWithNoFilter:

            def it_returns_the_correct_flow_rates_for_all_work_items(self, setup):
                fixture = setup

                query = """
                    query getProjectFlowRateTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [FlowRateTrends],
                            flowRateTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [
                                    arrival_rate,
                                    close_rate
                                ],
                            }
                        )
                        {
                            flowRateTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                                closeRate
                            }
                        }
                    }
            """

                client = Client(schema)

                result = client.execute(query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=15
                ))
                assert result['data']
                project = result['data']['project']
                assert len(project['flowRateTrends']) == 3
                flowRateTrends = project['flowRateTrends']
                for trends in flowRateTrends:
                    assert trends['closeRate'] == 0
                    if trends['arrivalRate'] != 0:
                        assert trends['arrivalRate'] == 3

        class TestWithFilters:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper
                # 1 issue 2 bugs
                api_helper.update_work_item(0, dict(work_item_type='issue', is_bug=False))
                api_helper.update_work_item(1, dict(work_item_type='bug', is_bug=True))
                api_helper.update_work_item(2, dict(work_item_type='bug', is_bug=True))
                # 1 issue with commits, 1 bug with commits, 1 bug with no commits
                api_helper.update_delivery_cycle(0, dict(commit_count=1))
                api_helper.update_delivery_cycle(1, dict(commit_count=2))
                api_helper.update_delivery_cycle(2, dict(commit_count=0))
                # 1 closed issue with commits, 1 closed bug with commits, 1 open bug without commits
                api_helper.update_delivery_cycle(0, dict(end_date=datetime.utcnow()))
                api_helper.update_delivery_cycle(1, dict(end_date=datetime.utcnow()))
                api_helper.update_delivery_cycle(2, dict(end_date=None))

                yield Fixture(
                    parent=fixture
                )

            class TestWithSpecsOnlyFilter:

                def it_returns_only_specs(self, setup):
                    fixture = setup

                    query = """
                                        query getProjectFlowRateTrends(
                                            $project_key:String!,
                                            $days: Int!,
                                            $window: Int!,
                                            $sample: Int
                                        ) {
                                            project(
                                                key: $project_key,
                                                interfaces: [FlowRateTrends],
                                                flowRateTrendsArgs: {
                                                    days: $days,
                                                    measurementWindow: $window,
                                                    samplingFrequency: $sample,
                                                    metrics: [
                                                        arrival_rate,
                                                        close_rate
                                                    ],
                                                    specsOnly: true
                                                }
                                            )
                                            {
                                                flowRateTrends {
                                                    measurementDate
                                                    measurementWindow
                                                    arrivalRate
                                                    closeRate
                                                }
                                            }
                                        }
                                """

                    client = Client(schema)

                    result = client.execute(query, variable_values=dict(
                        project_key=fixture.project.key,
                        days=11,
                        window=30,
                        sample=15
                    ))
                    assert result['data']
                    project = result['data']['project']
                    assert len(project['flowRateTrends']) == 1
                    trends = project['flowRateTrends'][0]
                    assert trends['closeRate'] == 2
                    assert trends['arrivalRate'] == 2

            class TestWithDefectsOnlyFilter:

                def it_returns_only_defects(self, setup):
                    fixture = setup

                    query = """
                                        query getProjectFlowRateTrends(
                                            $project_key:String!,
                                            $days: Int!,
                                            $window: Int!,
                                            $sample: Int
                                        ) {
                                            project(
                                                key: $project_key,
                                                interfaces: [FlowRateTrends],
                                                flowRateTrendsArgs: {
                                                    days: $days,
                                                    measurementWindow: $window,
                                                    samplingFrequency: $sample,
                                                    metrics: [
                                                        arrival_rate,
                                                        close_rate
                                                    ],
                                                    defectsOnly: true
                                                }
                                            )
                                            {
                                                flowRateTrends {
                                                    measurementDate
                                                    measurementWindow
                                                    arrivalRate
                                                    closeRate
                                                }
                                            }
                                        }
                                """

                    client = Client(schema)

                    result = client.execute(query, variable_values=dict(
                        project_key=fixture.project.key,
                        days=11,
                        window=30,
                        sample=15
                    ))
                    assert result['data']
                    project = result['data']['project']
                    assert len(project['flowRateTrends']) == 1
                    trends = project['flowRateTrends'][0]
                    assert trends['closeRate'] == 1
                    assert trends['arrivalRate'] == 2

            class TestWithSpecsOnlyDefectsOnlyFilters:

                def it_returns_defects_which_are_specs_too(self, setup):
                    fixture = setup

                    query = """
                                        query getProjectFlowRateTrends(
                                            $project_key:String!,
                                            $days: Int!,
                                            $window: Int!,
                                            $sample: Int
                                        ) {
                                            project(
                                                key: $project_key,
                                                interfaces: [FlowRateTrends],
                                                flowRateTrendsArgs: {
                                                    days: $days,
                                                    measurementWindow: $window,
                                                    samplingFrequency: $sample,
                                                    metrics: [
                                                        arrival_rate,
                                                        close_rate
                                                    ],
                                                    specsOnly: true,
                                                    defectsOnly: true
                                                }
                                            )
                                            {
                                                flowRateTrends {
                                                    measurementDate
                                                    measurementWindow
                                                    arrivalRate
                                                    closeRate
                                                }
                                            }
                                        }
                                """

                    client = Client(schema)

                    result = client.execute(query, variable_values=dict(
                        project_key=fixture.project.key,
                        days=11,
                        window=30,
                        sample=15
                    ))
                    assert result['data']
                    project = result['data']['project']
                    assert len(project['flowRateTrends']) == 1
                    trends = project['flowRateTrends'][0]
                    assert trends['closeRate'] == 1
                    assert trends['arrivalRate'] == 1

