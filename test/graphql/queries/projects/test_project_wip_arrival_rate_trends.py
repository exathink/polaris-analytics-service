# -*- coding: utf-8 -*-
import datetime

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client
from datetime import timedelta
from polaris.analytics.service.graphql import schema
from polaris.analytics.db import api
from polaris.analytics.db.model import WorkItem
from test.fixtures.project_work_items import *
from polaris.utils.collections import dict_merge

class TestProjectWipArrivalRateTrends(ProjectWorkItemsTest):
    class TestWipArrivalRate:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup
            organization = fixture.organization
            work_items_source = fixture.work_items_source
            work_items_common = fixture.work_items_common
            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id=f'ISSUE-{i}',
                    **work_items_common
                )
                for i in range(0,10)
            ]
            result = api.import_new_work_items(organization.key, work_items_source.key, work_items)
            assert not result.get('exception')

            yield Fixture(
                parent=fixture,
                work_items=work_items
            )

        def it_works(self, setup):
            fixture = setup
            query = """
                    query getProjectWipArrivalRateTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [WipArrivalRateTrends],
                            wipArrivalRateTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                            }
                        )
                        {
                            name
                            key
                            wipArrivalRateTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                            }
                        }
                    }
                    """
            client = Client(schema)

            result = client.execute(query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['wipArrivalRateTrends']) == 2

            assert [
                0,0
            ] == [
                measurement['arrivalRate']
                for measurement in project['wipArrivalRateTrends']
            ]
