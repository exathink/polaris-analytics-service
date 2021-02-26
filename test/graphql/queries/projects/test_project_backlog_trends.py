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


class TestProjectBacklogTrends:

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
            for i in range(0, 4)
        ]

        yield Fixture(
            work_items=work_items,
            project=project,
            api_helper=api_helper,
            start_date=start_date,
            output_attribute='backlogTrends'
        )

    class TestTrendingWindows(
        TrendingWindowMeasurementDate
    ):

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup

            query = """
                    query getProjectBacklogTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [BacklogTrends],
                            backlogTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [
                                    min_backlog_size
                                    max_backlog_size
                                    q1_backlog_size
                                    q3_backlog_size
                                    median_backlog_size
                                    avg_backlog_size
                                ],
                            }
                        )
                        {
                            backlogTrends {
                                measurementDate
                                measurementWindow
                                minBacklogSize
                                maxBacklogSize
                                q1BacklogSize
                                q3BacklogSize
                                medianBacklogSize
                                avgBacklogSize
                            }
                        }
                    }
            """
            yield Fixture(
                parent=fixture,
                query=query
            )

        class TestForAllWorkItems:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper
                # 4 work items of type bug, all with no commits, non-closed
                api_helper.import_work_items(fixture.work_items)
                # 2 delivery cycles set to old start date, and end_date a day ago
                api_helper.update_delivery_cycle(0, dict(start_date=fixture.start_date, end_date=datetime.utcnow()-timedelta(days=1)))
                api_helper.update_delivery_cycle(1, dict(start_date=fixture.start_date, end_date=datetime.utcnow()-timedelta(days=1)))
                # 2 work items and their delivery cycles updated to latest date
                api_helper.update_work_item(2, dict(created_at=datetime.utcnow()))
                api_helper.update_work_item(3, dict(created_at=datetime.utcnow()))
                api_helper.update_delivery_cycle(2, dict(start_date=datetime.utcnow()))
                api_helper.update_delivery_cycle(3, dict(start_date=datetime.utcnow()))

                yield Fixture(
                    parent=fixture
                )

            class TestWithNoFilter:

                def it_returns_the_correct_backlog_trends_for_all_work_items(self, setup):
                    fixture = setup

                    query = """
                        query getProjectBacklogTrends(
                            $project_key:String!,
                            $days: Int!,
                            $window: Int!,
                            $sample: Int
                        ) {
                            project(
                                key: $project_key,
                                interfaces: [BacklogTrends],
                                backlogTrendsArgs: {
                                    days: $days,
                                    measurementWindow: $window,
                                    samplingFrequency: $sample,
                                    metrics: [
                                        min_backlog_size
                                        max_backlog_size
                                        q1_backlog_size
                                        q3_backlog_size
                                        median_backlog_size
                                        avg_backlog_size
                                    ],
                                }
                            )
                            {
                                backlogTrends {
                                    measurementDate
                                    measurementWindow
                                    minBacklogSize
                                    maxBacklogSize
                                    q1BacklogSize
                                    q3BacklogSize
                                    medianBacklogSize
                                    avgBacklogSize
                                }
                            }
                        }
                """
                    client = Client(schema)

                    result = client.execute(query, variable_values=dict(
                        project_key=fixture.project.key,
                        days=15,
                        window=9,
                        sample=12
                    ))
                    assert result['data']
                    project = result['data']['project']
                    assert len(project['backlogTrends']) == 2
                    backlogTrends = project['backlogTrends']
                    for trends in backlogTrends:
                        if graphql_date(trends['measurementDate']).date() == datetime.utcnow().date():
                            assert trends['minBacklogSize'] == 2
                            assert trends['avgBacklogSize'] == 3
                        else:
                            assert trends['minBacklogSize'] == 4
                            assert trends['avgBacklogSize'] == 4
                        assert trends['maxBacklogSize'] == 4
                        assert trends['q1BacklogSize'] == 4
                        assert trends['medianBacklogSize'] == 4
                        assert trends['q3BacklogSize'] == 4
