# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client

from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestOrganizationInstance:
    def it_implements_the_named_node_interface(self, org_repo_fixture):
        client = Client(schema)
        query = """
                    query getOrganization($organization_key:String!) {
                        organization(key: $organization_key) {
                            id
                            name
                            key
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        organization = result['data']['organization']
        assert organization['id']
        assert organization['name']
        assert organization['key']

    def it_implements_the_commit_summary_interface(self, api_import_commits_fixture):
        client = Client(schema)
        query = """
                    query getOrganization($organization_key:String!) {
                        organization(key: $organization_key, interfaces: [CommitSummary]) {
                            ... on CommitSummary {
                                earliestCommit
                                latestCommit
                                commitCount
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        organization = result['data']['organization']
        assert organization['earliestCommit'] == '2019-10-01T00:00:00'
        assert organization['latestCommit'] == '2019-11-02T00:00:00'
        assert organization['commitCount'] == 4


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
                              stateType
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
            assert node['stateType']

    def it_implements_the_project_work_item_info_interface(self, project_fixture):
        _, _, _, project = project_fixture
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
                              tags
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
            assert node['stateType']

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
                              stateType 
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
                                stateType
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
            assert node['stateType']

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
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key,
                                                            before=get_date("2018-12-03").isoformat()))
        assert 'data' in result
        edges = result['data']['organization']['workItemEvents']['edges']
        assert len(edges) == 3
        for node in map(lambda edge: edge['node'], edges):
            key, seq_no = node['key'].split(':')
            assert uuid.UUID(key).hex in keys


class TestOrganizationWorkItemCommits:

    def it_implements_the_named_node_interface(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemCommits {
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
        edges = result['data']['organization']['workItemCommits']['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            assert node['id']
            assert node['name']
            assert node['key']

    def it_returns_composite_event_keys(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemCommits {
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
        edges = result['data']['organization']['workItemCommits']['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            composite_key = node['key'].split(':')
            assert len(composite_key) == 2

    def it_returns_unique_event_ids_using_the_composite_keys(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemCommits {
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
        edges = result['data']['organization']['workItemCommits']['edges']
        assert len(edges) == 2
        ids = set(map(lambda edge: edge['node']['id'], edges))
        assert len(ids) == 2

    def it_implements_the_work_item_source_ref_interface(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemCommits {
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
        edges = result['data']['organization']['workItemCommits']['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            assert node['workItemsSourceName']
            assert node['workItemsSourceKey']

    def it_implements_the_work_item_info_interface(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemCommits {
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
                                stateType
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItemCommits']['edges']
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
            assert node['stateType']

    def it_implements_the_work_item_commit_info_interface(self, setup_work_item_transitions):
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key) {
                    workItemCommits {
                        edges {
                            node {
                                workItemName
                                workItemKey
                                commitKey
                                commitHash
                                repository
                                repositoryKey
                                repositoryUrl
                                commitDate
                                committer
                                committerKey
                                authorDate
                                author
                                authorKey
                                commitMessage
                                
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        edges = result['data']['organization']['workItemCommits']['edges']
        assert len(edges) == 2
        for node in map(lambda edge: edge['node'], edges):
            assert node['commitKey']
            assert node['commitHash']
            assert node['repository']
            assert node['repositoryKey']
            assert node['repositoryUrl']
            assert node['commitDate']
            assert node['committer']
            assert node['committerKey']
            assert node['authorDate']
            assert node['author']
            assert node['authorKey']
            assert node['commitMessage']
            assert node['workItemName']
            assert node['workItemKey']


class TestOrganizationWorkItemEventSpan:

    def it_implements_the_work_item_event_span_interface(self, work_items_fixture):
        work_item_key, _, _ = work_items_fixture
        client = Client(schema)
        query = """
            query getOrganizationWorkItems($organization_key:String!) {
                organization(key: $organization_key, interfaces: [WorkItemEventSpan]) {
                    earliestWorkItemEvent
                    latestWorkItemEvent
                }
            }
        """
        result = client.execute(query, variable_values=dict(organization_key=test_organization_key))
        assert 'data' in result
        assert result['data']['organization']['earliestWorkItemEvent']
        assert result['data']['organization']['latestWorkItemEvent']
