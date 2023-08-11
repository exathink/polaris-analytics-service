# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from graphene.test import Client
from datetime import timedelta, datetime
from polaris.analytics.service.graphql import schema
from polaris.analytics.db import api
from polaris.analytics.db.model import WorkItem
from test.fixtures.project_work_items import *
from polaris.utils.collections import dict_merge
from test.fixtures.graphql import WorkItemImportApiHelper

class TestProjectReleases(ProjectWorkItemsTest):
    class TestProjectAllReleases:
        def it_returns_all_unique_project_releases(self, setup):
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

        def it_sorts_releases_in_descending_order_by_name(self, setup):
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
            assert releases == ['release4', 'release3', 'release2', 'release1']

        def it_supports_the_active_within_days_flag(self, setup):
            fixture = setup

            organization = fixture.organization
            work_items_source = fixture.work_items_source
            work_items_common = fixture.work_items_common
            start_date = datetime.utcnow() - timedelta(days=90)
            api_helper = WorkItemImportApiHelper(organization,work_items_source)
            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='ISSUE-1',
                    **dict_merge(
                        work_items_common,
                        dict(
                            created_at=start_date,
                            updated_at=start_date,
                            releases=['release1'],
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
                            created_at=start_date,
                            updated_at=start_date,
                            releases=[ 'release2'],
                        )
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 3',
                    display_id='ISSUE-2',
                    **dict_merge(
                        work_items_common,
                        dict(
                            releases=['release3'],
                        )
                    )
                )
            ]
            result = api_helper.import_work_items(work_items)

            # closed T+20 => Release 1
            api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=10))])
            api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=20))])

            # closed T+40 => Release 2
            api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=20))])
            api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=40))])

            # closed T+60 => Release 3
            api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=30))])
            api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=60))])
            project = fixture.project

            client = Client(schema)
            query = '''
            query getProjectReleases($projectKey: String!, $releasesWindow: Int!) {
                project(key: $projectKey, interfaces: [Releases], releasesActiveWithinDays: $releasesWindow) {
                    releases 
                }
            }
            '''
            result = client.execute(query, variable_values=dict(
                projectKey=str(project.key),
                releasesWindow=60
            ))
            assert not result.get('errors')
            releases = result['data']['project']['releases']
            assert len(releases) == 2
            assert set(result['data']['project']['releases']) == set(['release3', 'release2'])

            result = client.execute(query, variable_values=dict(
                projectKey=str(project.key),
                releasesWindow=40
            ))
            assert not result.get('errors')
            releases = result['data']['project']['releases']
            assert len(releases) == 1
            assert set(result['data']['project']['releases']) == set(['release3'])

            result = client.execute(query, variable_values=dict(
                projectKey=str(project.key),
                releasesWindow=30
            ))
            assert not result.get('errors')
            releases = result['data']['project']['releases']
            assert len(releases) == 0


        def it_returns_an_empty_list_when_there_are_no_releases(self, setup):
            fixture = setup

            organization = fixture.organization
            work_items_source = fixture.work_items_source
            work_items_common = fixture.work_items_common
            result = api.import_new_work_items(organization.key, work_items_source.key, [
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='ISSUE-1',
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='ISSUE-2',
                    **work_items_common
                )
            ])
            assert not result.get('exception')
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
            assert len(releases) == 0
