# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client

from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from datetime import datetime, timedelta

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
                    stateType
                    isBug
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
        assert work_item['stateType'] == work_items_common['state_type']
        assert work_item['isBug'] == work_items_common['is_bug']

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
        assert work_item['earliestCommit'] == get_date("2020-01-29").isoformat()
        assert work_item['latestCommit'] == get_date("2020-02-05").isoformat()
        assert work_item['commitCount'] == 2

    def it_implements_work_item_event_span_interface(self, setup_work_item_transitions):
        new_work_items = setup_work_item_transitions
        work_item_key = new_work_items[0]['key']

        client = Client(schema)
        query = """
                    query getWorkItem($key:String!) {
                        workItem(key: $key, interfaces: [WorkItemEventSpan]){
                            earliestWorkItemEvent
                            latestWorkItemEvent
                        }
                    } 
                """
        result = client.execute(query, variable_values=dict(key=work_item_key))
        assert 'data' in result
        work_item = result['data']['workItem']
        assert work_item
        assert work_item['earliestWorkItemEvent'] == '2018-12-02T00:00:00'
        assert work_item['latestWorkItemEvent'] == '2018-12-03T00:00:00'


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
                                            previousStateType
                                            newState
                                            newStateType
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
                assert node['newStateType']
                if node['seqNo'] == 1:
                    assert node['previousState']
                    assert node['previousStateType']



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

    class TestWorkItemInstanceWorkItemStateDetails:

        def it_returns_current_state_transition(self, setup_work_item_transitions):
            new_work_items = setup_work_item_transitions
            work_item_key = new_work_items[0]['key']

            client = Client(schema)
            query = """
                        query getWorkItem($key:String!) {
                            workItem(key: $key, interfaces:[WorkItemStateDetails]){
                                ... on WorkItemStateDetails {
                                    workItemStateDetails {
                                        currentStateTransition {
                                            eventDate
                                            
                                        }
                                    }
                                }
                            }
                        } 
                    """
            result = client.execute(query, variable_values=dict(key=work_item_key))
            assert 'data' in result
            work_item_state_details = result['data']['workItem']['workItemStateDetails']
            assert work_item_state_details['currentStateTransition']['eventDate']

        def it_returns_current_delivery_cycle_durations(self, api_work_items_import_fixture):
            organization, project, work_items_source, work_items_common = api_work_items_import_fixture
            api_helper = WorkItemImportApiHelper(organization, work_items_source)

            work_item_key = uuid.uuid4().hex
            start_date = datetime.utcnow() - timedelta(days=10)
            api_helper.import_work_items([
                    dict(
                        key=work_item_key,
                        name='Issue 1',
                        display_id='1000',
                        state='backlog',
                        created_at=start_date,
                        updated_at=start_date,
                        **work_items_common
                    )
                ]
            )

            api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
            api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])

            client = Client(schema)
            query = """
                    query getWorkItem($key:String!) {
                        workItem(key: $key, interfaces:[WorkItemStateDetails]){
                            ... on WorkItemStateDetails {
                                workItemStateDetails {
                                    currentStateTransition {
                                        eventDate
                                    }
                                    currentDeliveryCycleDurations {
                                        state
                                        stateType
                                        daysInState
                                    }
                                }
                            }
                        }
                    } 
                """
            result = client.execute(query, variable_values=dict(key=work_item_key))
            assert 'data' in result
            work_item_state_details = result['data']['workItem']['workItemStateDetails']
            assert work_item_state_details['currentStateTransition']['eventDate']
            assert {
                (record['state'], record['daysInState'])
                for record in work_item_state_details['currentDeliveryCycleDurations']
            } == {
                ('created', 0.0),
                ('backlog', 1.0),
                ('upnext', 1.0),
                ('doing', 2.0),
                ('done', None)
            }

        def it_returns_null_state_types_when_there_are_unmapped_durations(self, api_work_items_import_fixture):
            organization, project, work_items_source, work_items_common = api_work_items_import_fixture
            api_helper = WorkItemImportApiHelper(organization, work_items_source)

            work_item_key = uuid.uuid4().hex
            start_date = datetime.utcnow() - timedelta(days=10)
            api_helper.import_work_items([
                    dict(
                        key=work_item_key,
                        name='Issue 1',
                        display_id='1000',
                        state='backlog',
                        created_at=start_date,
                        updated_at=start_date,
                        **work_items_common
                    )
                ]
            )

            api_helper.update_work_items([(0, 'unmapped_state', start_date + timedelta(days=1))])


            client = Client(schema)
            query = """
                    query getWorkItem($key:String!) {
                        workItem(key: $key, interfaces:[WorkItemStateDetails]){
                            ... on WorkItemStateDetails {
                                workItemStateDetails {
                                    currentStateTransition {
                                        eventDate
                                    }
                                    currentDeliveryCycleDurations {
                                        state
                                        stateType
                                        daysInState
                                    }
                                }
                            }
                        }
                    } 
                """
            result = client.execute(query, variable_values=dict(key=work_item_key))
            assert 'data' in result
            work_item_state_details = result['data']['workItem']['workItemStateDetails']
            assert work_item_state_details['currentStateTransition']['eventDate']
            assert {
                (record['state'], record['stateType'], record['daysInState'])
                for record in work_item_state_details['currentDeliveryCycleDurations']
            } == {
                ('created', 'backlog', 0.0),
                ('backlog', 'backlog', 1.0),
                ('unmapped_state', None,  None)
            }



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



