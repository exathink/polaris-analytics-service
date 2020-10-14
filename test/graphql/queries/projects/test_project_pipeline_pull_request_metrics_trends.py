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
            for i in range(0, 3)
        ]

        pull_requests = [
            dict(
                repository_id=repositories[0].id,
                key=uuid.uuid4(),
                source_id='1000',
                source_repository_id=repositories[0].id,
                title="Another change. Fixes issue #1000",
                created_at=start_date,
                **pull_requests_common
            )
            for i in range(0, 3)
        ]

        yield Fixture(
            project=project,
            api_helper=api_helper,
            work_items=work_items,
            pull_requests=pull_requests,
            repositories=repositories
        )


    class TestPullRequestMetrics:

        @pytest.yield_fixture()
        def setup(self):
            metrics_query = """
            """


        class TestWhenWorkItemIsOpen:

            class TestWhenDeliveryCycleIsOpen:

                class TestWhenTwoOpenNoClosedPullRequests:
                    pass

                class TestWhenNoOpenPullRequests:
                    pass

                class TestWhenOneOpenOneClosedPullRequests:
                    pass

                class TestWhenNoPullRequests:
                    pass

            class TesWhenDeliveryCycleIsClosed:
                class TestWhenTwoOpenNoClosedPullRequests:
                    pass

                class TestWhenNoOpenPullRequests:
                    pass

                class TestWhenOneOpenOneClosedPullRequests:
                    pass


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

        class TestWhenPullRequestIsLinkedToTwoWorkItems:
            pass


