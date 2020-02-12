# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.graphql import *
from graphene.test import Client
from polaris.analytics.service.graphql import schema


class TestWorkItemInstance:

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
        assert workItem['name'] == 'Issue 1'
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
                    stateType
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
        assert work_item['stateType'] == work_items_common['state_type']

    def it_implements_commit_summary_info_interface(self, work_items_commit_summary_fixture):
        work_item_key, _, _ = work_items_commit_summary_fixture
        client = Client(schema)
        query = """
            query getWorkItem($key:String!) {
                workItem(key: $key, interfaces: [WorkItemInfo, CommitSummary]){
                    description
                    displayId
                    state
                    workItemType
                    updatedAt
                    url
                    tags
                    earliestCommit
                    latestCommit
                    commitCount
                }
            } 
        """
        result = client.execute(query, variable_values=dict(key=work_item_key))

        assert 'data' in result
        work_item = result['data']['workItem']
        assert work_item['displayId'] == '1002'
        assert work_item['description'] == work_items_common['description']
        assert work_item['state'] == work_items_common['state']
        assert work_item['workItemType'] == work_items_common['work_item_type']
        assert work_item['updatedAt'] == get_date("2018-12-03").isoformat()
        assert work_item['url'] == work_items_common['url']
        assert work_item['tags'] == work_items_common['tags']
        assert work_item['earliestCommit'] == get_date("2020-01-29").isoformat()
        assert work_item['latestCommit'] == get_date("2020-02-05").isoformat()
        assert work_item['commitCount'] == 2

    class TestWorkItemInstanceEvents:

        def it_returns_work_item_event_named_nodes(self, setup_work_item_transitions):
            new_work_items = setup_work_item_transitions
            work_item_key = new_work_items[0]['key']

            client = Client(schema)
            query = """
                        query getWorkItem($key:String!) {
                            workItem(key: $key){
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
            result = client.execute(query, variable_values=dict(key=work_item_key))
            assert 'data' in result
            edges = result['data']['workItem']['workItemEvents']['edges']
            assert len(edges) == 2
            # unique event id test
            assert len(set(map(lambda edge: edge['node']['id'], edges))) == 2
            # all events have the same name as the work item
            assert set(map(lambda edge: edge['node']['name'], edges)) == {'Issue 1'}
            # all events have compound keys

            assert set(map(lambda edge: edge['node']['key'], edges)) == {f'{uuid.UUID(work_item_key)}:0', f'{uuid.UUID(work_item_key)}:1'}

        def it_returns_work_item_events_state_transitions(self, setup_work_item_transitions):
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
            edges = result['data']['workItem']['workItemEvents']['edges']
            assert len(edges) == 2
            for node in map(lambda edge: edge['node'], edges):
                assert node['seqNo'] is not None
                assert node['eventDate']
                assert node['newState']


        def it_returns_work_item_events_source_refs(self, setup_work_item_transitions):
            new_work_items = setup_work_item_transitions
            work_item_key = new_work_items[0]['key']

            client = Client(schema)
            query = """
                        query getWorkItem($key:String!) {
                            workItem(key: $key){
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
            result = client.execute(query, variable_values=dict(key=work_item_key))
            assert 'data' in result
            edges = result['data']['workItem']['workItemEvents']['edges']
            assert len(edges) == 2
            for node in map(lambda edge: edge['node'], edges):
                assert node['workItemsSourceName']
                assert node['workItemsSourceKey']


    class TestWorkItemInstanceCommits:

        def it_returns_work_item_commits_named_nodes(self, work_items_fixture):
            work_item_key, _, _ = work_items_fixture
            test_repo = getRepository('alpha')

            client = Client(schema)
            query = """
                        query getWorkItem($key:String!) {
                            workItem(key: $key){
                                commits {
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
            result = client.execute(query, variable_values=dict(key=work_item_key))
            assert 'data' in result
            edges = result['data']['workItem']['commits']['edges']
            assert len(edges) == 2
            # unique commit id test
            assert len(set(map(lambda edge: edge['node']['id'], edges))) == 2
            # all commits are named by the source_commit_id
            assert set(map(lambda edge: edge['node']['name'], edges)) == {'XXXXXX', 'YYYYYY'}
            # all commits have the key of form repository_key:source_commit_id

            assert set(map(lambda edge: edge['node']['key'], edges)) == {f'{test_repo.key}:XXXXXX', f'{test_repo.key}:YYYYYY'}



