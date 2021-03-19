# -*- coding: utf-8 -*-
# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from test.fixtures.graphql import WorkItemImportApiHelper


class TestProjectWorkItems:
    def it_implements_the_work_item_info_interface(self, commit_summary_fixture):
        _, _, _, project = commit_summary_fixture
        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!) {
                project(key: $project_key) {
                    workItems {
                        edges {
                            node {
                              description
                              displayId
                              state
                              workItemType
                              createdAt
                              updatedAt
                              url
                              stateType
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert 'data' in result
        edges = result['data']['project']['workItems']['edges']
        assert len(edges) == 1
        for node in map(lambda edge: edge['node'], edges):
            assert node['description']
            assert node['displayId']
            assert node['state']
            assert node['workItemType']
            assert node['url']
            assert node['updatedAt']
            assert node['createdAt']
            assert node['stateType']

    def it_implements_the_commit_summary_interface(self, commit_summary_fixture):
        _, _, _, project = commit_summary_fixture
        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!) {
                project(key: $project_key) {
                    workItems(interfaces: [CommitSummary]) {
                        edges {
                            node {
                              description
                              displayId
                              state
                              workItemType
                              url
                              earliestCommit
                              latestCommit
                              commitCount
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert 'data' in result
        edges = result['data']['project']['workItems']['edges']
        assert len(edges) == 1
        for node in map(lambda edge: edge['node'], edges):
            assert node['description'] == work_items_common['description']
            assert node['displayId'] == "1001"
            assert node['state'] == work_items_common['state']
            assert node['workItemType'] == work_items_common['work_item_type']
            assert node['url'] == work_items_common['url']
            assert node['earliestCommit'] == get_date("2020-01-29").isoformat()
            assert node['latestCommit'] == get_date("2020-02-05").isoformat()
            assert node['commitCount'] == 2

    def it_respects_the_defects_only_parameter(self, api_work_items_import_fixture):
        organization, project, work_items_source, _ = api_work_items_import_fixture

        work_items_common = dict(
            work_item_type='issue',
            url='http://foo.com',
            tags=['ares2'],
            description='foo',
            source_id=str(uuid.uuid4()),
            is_epic=False,
            parent_id=None,
        )

        api.import_new_work_items(
            organization_key=organization.key,
            work_item_source_key=work_items_source.key,
            work_item_summaries=[
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='1000',
                    is_bug=False,
                    state='backlog',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='1001',
                    is_bug=True,
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 3',
                    display_id='1002',
                    state='upnext',
                    is_bug=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 4',
                    display_id='1004',
                    state='doing',
                    is_bug=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 5',
                    display_id='1005',
                    state='doing',
                    is_bug=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 6',
                    display_id='1006',
                    state='closed',
                    is_bug=True,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),

            ]
        )

        client = Client(schema)
        query = """
                    query getProjectDefects($project_key:String!) {
                        project(key: $project_key) {
                            workItems(defectsOnly: true) {
                                edges {
                                    node {
                                      id
                                      name
                                      key
                                    }
                                }
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert 'data' in result
        assert len(result['data']['project']['workItems']['edges']) == 2

    def it_respects_the_specs_only_flag(self, api_work_items_import_fixture):
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
        api_helper.import_work_items(work_items)
        api_helper.update_delivery_cycles(([(0, dict(property='commit_count', value=3))]))

        client = Client(schema)
        query = """
                query getProjectDefects($project_key:String!) {
                    project(key: $project_key) {
                        workItems(specsOnly: true) {
                            edges {
                                node {
                                  id
                                  name
                                  key
                                }
                            }
                        }
                    }
                }
                        """
        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert 'data' in result
        assert len(result['data']['project']['workItems']['edges']) == 1
