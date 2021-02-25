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
