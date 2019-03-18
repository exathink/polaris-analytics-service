# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.graphql import *
import pytest
from graphene.test import Client
from polaris.analytics.service.graphql import schema


work_items_common = dict(
    name='Issue',
    is_bug=True,
    work_item_type='issue',
    url='http://foo.com',
    tags=['ares2'],
    state='open',
    description='foo'

)


@pytest.yield_fixture
def work_items_fixture(commits_fixture):
    organization, _, repositories, _ = commits_fixture
    test_repo = repositories['alpha']
    new_key = uuid.uuid4()
    new_work_items = [
        dict(
            key=new_key.hex,
            display_id='1000',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            **work_items_common
        ),
        dict(
            key=uuid.uuid4().hex,
            display_id='2000',
            created_at=get_date("2018-12-03"),
            updated_at=get_date("2018-12-04"),
            **work_items_common
        ),

    ]
    create_work_items(
        organization,
        source_data=dict(
            integration_type='github',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=test_repo.key,
            **work_items_source_common
        ),
        items_data=new_work_items
    )
    test_commit_source_id = '00001'
    test_commit_key = uuid.uuid4()
    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=test_commit_key.hex,
            source_commit_id=test_commit_source_id,
            commit_message="Another change. Fixes issue #1000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
    ]
    create_test_commits(test_commits)
    yield new_key, test_commit_key, new_work_items


@pytest.yield_fixture
def setup_work_item_transitions(work_items_fixture):
    new_key, test_commit_key, new_work_items = work_items_fixture
    key = new_work_items[0]['key']
    create_transitions(key, [
        dict(
            seq_no=0,
            previous_state=None,
            state='open',
            created_at=new_work_items[0]['created_at']
        ),
        dict(
            seq_no=1,
            previous_state='open',
            state='closed',
            created_at=new_work_items[0]['updated_at']
        ),

    ])
    key = new_work_items[1]['key']
    create_transitions(key, [
        dict(
            seq_no=0,
            previous_state=None,
            state='open',
            created_at=new_work_items[1]['created_at']
        ),
        dict(
            seq_no=1,
            previous_state='open',
            state='closed',
            created_at=new_work_items[1]['updated_at']
        ),
    ])

    yield new_work_items

    db.connection().execute("delete from analytics.work_item_state_transitions")


class TestWorkItemQueries:

    def it_implements_named_node_interface(self, work_items_fixture):
        work_item_key, _, _ = work_items_fixture
        client = Client(schema)
        query = """
            query getWorkItem($key:String!) {
                workItem(key: $key){
                    id
                    name
                    key
                }
            } 
        """
        result = client.execute(query, variable_values=dict(key=work_item_key))
        assert 'data' in result
        workItem = result['data']['workItem']
        assert workItem['id']
        assert workItem['name'] == 'Issue'
        assert workItem['key'] == str(work_item_key)


    def it_implements_work_item_info_interface(self, work_items_fixture):
        work_item_key, _ , _= work_items_fixture
        client = Client(schema)
        query = """
            query getWorkItem($key:String!) {
                workItem(key: $key, interfaces: [WorkItemInfo]){
                    description
                    displayId
                    state
                    workItemType
                    updatedAt
                    url
                    tags
                }
            } 
        """
        result = client.execute(query, variable_values=dict(key=work_item_key))
        assert 'data' in result
        work_item = result['data']['workItem']
        assert work_item['displayId'] == '1000'
        assert work_item['description'] == work_items_common['description']
        assert work_item['state'] == work_items_common['state']
        assert work_item['workItemType'] == work_items_common['work_item_type']
        assert work_item['updatedAt'] == get_date("2018-12-03").isoformat()
        assert work_item['url'] == work_items_common['url']
        assert work_item['tags'] == work_items_common['tags']

    def it_returns_work_item_events(self, setup_work_item_transitions):
        new_work_items = setup_work_item_transitions
        work_item_key = new_work_items[0]['key']

        client = Client(schema)
        query = """
                    query getWorkItem($key:String!) {
                        workItem(key: $key){
                            workItemEvents {
                                edges {
                                    node {
                                        seqNo
                                        eventDate
                                        previousState
                                        newState
                                    }
                                }
                            }
                        }
                    } 
                """
        result = client.execute(query, variable_values=dict(key=work_item_key))
        assert 'data' in result
        work_item_events = result['data']['workItem']['workItemEvents']
        assert len(work_item_events['edges']) == 2


class TestOrganizationWorkItems:

    def it_implements_the_named_node_interface(self, work_items_fixture):
        work_item_key, _, _ = work_items_fixture
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItems {
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
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItems']['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            assert node['id']
            assert node['name']
            assert node['key']


    def it_implements_the_work_item_info_interface(self, work_items_fixture):
        work_item_key, _, _ = work_items_fixture
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
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
                              tags  
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItems']['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            assert node['description']
            assert node['displayId']
            assert node['state']
            assert node['workItemType']
            assert node['tags']
            assert node['url']
            assert node['updatedAt']
            assert node['createdAt']

    def it_supports_paging(self, work_items_fixture):
        work_item_key, _, _ = work_items_fixture
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItems(first: 1) {
                        edges {
                            node {
                              description
                              displayId
                              state
                              workItemType
                              createdAt
                              updatedAt
                              url
                              tags  
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItems']['edges']
        assert len(edges) == 1

    def it_returns_pages_in_descending_order_of_updated_at_dates(self, work_items_fixture):
        work_item_key, _, new_work_items = work_items_fixture
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItems(first: 1) {
                        edges {
                            node {
                              key
                              updatedAt
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItems']['edges']
        assert len(edges) == 1
        assert uuid.UUID(edges[0]['node']['key']) == uuid.UUID(new_work_items[1]['key'])


class TestOrganizationWorkItemEvents:

    def it_implements_the_named_node_interface(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemEvents {
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
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItemEvents']['edges']
        assert len(edges) == 4
        for node in map(lambda edge: edge['node'], edges):
            assert node['id']
            assert node['name']
            assert node['key']

    def it_returns_composite_event_keys(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemEvents {
                        edges {
                            node {
                                key
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItemEvents']['edges']
        assert len(edges) == 4
        for node in map(lambda edge: edge['node'], edges):
            composite_key = node['key'].split(':')
            assert len(composite_key) == 2

    def it_returns_unique_event_ids_using_the_composite_keys(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemEvents {
                        edges {
                            node {
                                id
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItemEvents']['edges']
        assert len(edges) == 4
        ids = set(map(lambda edge: edge['node']['id'], edges))
        assert len(ids) == 4

    def it_implements_the_work_item_info_interface(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemEvents {
                        edges {
                            node {
                                description
                                displayId
                                state
                                workItemType
                                createdAt
                                updatedAt
                                url
                                tags  
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItemEvents']['edges']
        assert len(edges) == 4
        for node in map(lambda edge: edge['node'], edges):
            assert node['description']
            assert node['displayId']
            assert node['state']
            assert node['workItemType']
            assert node['tags']
            assert node['url']
            assert node['updatedAt']
            assert node['createdAt']


    def it_implements_the_work_item_source_ref_interface(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemEvents {
                        edges {
                            node {
                                workItemsSourceName
                                workItemsSourceKey
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItemEvents']['edges']
        assert len(edges) == 4
        for node in map(lambda edge: edge['node'], edges):
            assert node['workItemsSourceName']
            assert node['workItemsSourceKey']


    def it_supports_filtering_events_by_days(self, setup_work_item_transitions):
        work_items = setup_work_item_transitions

        work_item_key = work_items[0]['key']
        create_transitions(work_item_key, [
            dict(seq_no=3, created_at=datetime.utcnow(), previous_state=None, state='accepted')
        ])

        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemEvents(days:1) {
                        edges {
                            node {
                                id
                                key
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItemEvents']['edges']
        assert len(edges) == 1
        for node in map(lambda edge: edge['node'], edges):
            key, seq_no = node['key'].split(':')
            assert uuid.UUID(key).hex == work_item_key


    def it_supports_filtering_events_by_before(self, setup_work_item_transitions):
        work_items = setup_work_item_transitions

        keys = [work_item['key'] for work_item in work_items]

        create_transitions(keys[0], [
            dict(seq_no=3, created_at=datetime.utcnow(), previous_state=None, state='accepted')
        ])

        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!, $before: DateTime) {
                organization(key: $organization_key) {
                    workItemEvents(before: $before) {
                        edges {
                            node {
                                key
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key, before=get_date("2018-12-03").isoformat()))
        assert 'data' in result
        edges = result['data']['organization']['workItemEvents']['edges']
        assert len(edges) == 3
        for node in map(lambda edge: edge['node'], edges):
            key, seq_no = node['key'].split(':')
            assert uuid.UUID(key).hex in keys

