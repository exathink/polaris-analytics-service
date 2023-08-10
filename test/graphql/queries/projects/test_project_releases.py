# -*- coding: utf-8 -*-

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


class TestProjectReleases(ProjectWorkItemsTest):
    class TestProjectAllReleases:

        @pytest.fixture()
        def setup(self, setup):
            fixture = setup
            organization = fixture.organization
            work_items_source = fixture.work_items_source
            work_items_common = fixture.work_items_common
            result = api.import_new_work_items(organization.key, work_items_source.key, [
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='ISSUE-1',
                    **dict_merge(
                        work_items_common,
                        dict(
                            releases=['release1', 'release2', 'release3'],
                        )
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='ISSUE-2',
                    **dict_merge(
                        work_items_common,
                        dict(
                            releases=['release1', 'release2', 'release4'],
                        )
                    )
                )
            ]
                                               )
            assert not result.get('exception')
            yield fixture

        def it_returns_all_unique_project_releases(self, setup):
            fixture = setup
            project = fixture.project

            client = Client(schema)
            query = '''
            query getProjectReleases($projectKey: String!) {
                project(key: $projectKey, interfaces: [Releases]) {
                    releases 
                }
            }
            '''
            result = client.execute(query, variable_values={'projectKey': str(project.key)})
            assert not result.get('errors')
            releases = result['data']['project']['releases']
            assert len(releases) == 4
            assert set(result['data']['project']['releases']) == set(['release1', 'release2', 'release3', 'release4'])
