# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.graphql import *
from graphene.test import Client
from polaris.analytics.service.graphql import schema


class TestWorkItemsSourceInstance:

    def it_implements_named_node_interface(self, work_items_sources_fixture):
        work_items_source_key, _ = work_items_sources_fixture
        client = Client(schema)
        query = """
            query getWorkItemsSource($key:String!) {
                workItemsSource(key: $key){
                    id
                    name
                    key
                }
            }
        """
        result = client.execute(query, variable_values=dict(key=work_items_source_key))
        assert 'data' in result
        workItemsSource = result['data']['workItemsSource']
        assert workItemsSource['id']
        assert workItemsSource['name'] == 'Test Work Items Source 1'
        assert workItemsSource['key'] == str(work_items_source_key)

class TestWorkItemsSourceWorkItems:
    def it_returns_work_items(self, work_items_sources_work_items_fixture):
        work_items_source_key, _ = work_items_sources_work_items_fixture
        client = Client(schema)
        query = """
            query getWorkItemsSource($key:String!) {
                workItemsSource(key: $key){
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
        result = client.execute(query, variable_values=dict(key=work_items_source_key))
        assert 'data' in result
        workItemsSource = result['data']['workItemsSource']
        workItems = workItemsSource['workItems']
        edges = workItems['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            assert node['id']
            assert node['name']
            assert node['key']

    def it_implements_work_items_pagination(self, work_items_sources_work_items_fixture):
        work_items_source_key, _ = work_items_sources_work_items_fixture
        client = Client(schema)
        query = """
            query getWorkItemsSource($key:String!) {
                workItemsSource(key: $key){
                    workItems(first:1) {
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
        result = client.execute(query, variable_values=dict(key=work_items_source_key))
        assert 'data' in result
        workItemsSource = result['data']['workItemsSource']
        workItems = workItemsSource['workItems']
        edges = workItems['edges']
        assert len(edges) == 1
        for node in map(lambda edge: edge['node'], edges):
            assert node['id']
            assert node['name']
            assert node['key']

    def it_implements_work_items_info_interface(self, work_items_sources_work_items_fixture):
        work_items_source_key, _ = work_items_sources_work_items_fixture
        client = Client(schema)
        query = """
            query getWorkItemsSource($key:String!) {
                workItemsSource(key: $key){
                    workItems(interfaces: [WorkItemInfo]) {
                        edges {
                        node {
                          id
                          name
                          key
                          ... on WorkItemInfo {
                            description
                          }
                        }
                      }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(key=work_items_source_key))
        assert 'data' in result
        workItemsSource = result['data']['workItemsSource']
        workItems = workItemsSource['workItems']
        edges = workItems['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            assert node['id']
            assert node['name']
            assert node['key']
            assert node['description']

class TestWorkItemsSourceWorkItemCommits:
    def it_returns_work_item_events(self, work_items_sources_work_items_fixture):
        work_items_source_key, _ = work_items_sources_work_items_fixture
        client = Client(schema)
        query = """
            query getWorkItemsSource($key:String!) {
                workItemsSource(key: $key){
                    workItemCommits{
                      edges {
                        node {
                          id
                          name
                          key
                          commitDate
                          commitMessage
                        }
                      }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(key=work_items_source_key))
        assert 'data' in result
        workItemsSource = result['data']['workItemsSource']
        workItemCommits = workItemsSource['workItemCommits']
        edges = workItemCommits['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            assert node['id']
            assert node['name']
            assert node['key']
            assert node['commitDate']
            assert node['commitMessage']
