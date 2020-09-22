from graphene.test import Client
from polaris.analytics.db.enums import JiraWorkItemType
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *

from test.graphql.queries.projects.shared_testing_mixins import \
    TrendingWindowTestNumberOfMeasurements, \
    TrendingWindowMeasurementDate

class TestProjectFlowMixTrends:

    @pytest.yield_fixture
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
            project=project,
            api_helper=api_helper,
            work_items=work_items,
            start_date=start_date,
        )

    class TestMeasurementWindowContracts(
        TrendingWindowTestNumberOfMeasurements,
        TrendingWindowMeasurementDate
    ):

        @pytest.yield_fixture
        def setup(self, setup):
            fixture = setup
            measurements_query = """
                query getProjectFlowMixTrends(
                    $project_key:String!, 
                    $days: Int!, 
                    $window: Int!,
                    $sample: Int,
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [FlowMixTrends], 
                        flowMixTrendsArgs: {
                            days: $days,
                            measurementWindow: $window,
                            samplingFrequency: $sample,
                        }

                    ) {
                        flowMixTrends {
                            measurementDate
                            measurementWindow
                        }
                    }
                }
            """

            yield Fixture(
                parent=fixture,
                query=measurements_query,
                output_attribute='flowMixTrends'
            )