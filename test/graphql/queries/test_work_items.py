# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta

from graphene.test import Client
from polaris.analytics.db.model import WorkItemDeliveryCycleContributor, ContributorAlias
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from test.fixtures.work_items_commits import *


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
        work_item_key, _, _ = work_items_fixture
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

    def it_implements_work_items_source_ref_interface(self, work_items_fixture):
        work_item_key, _, _ = work_items_fixture
        client = Client(schema)
        query = """
            query getWorkItem($key:String!) {
                workItem(key: $key, interfaces: [WorkItemsSourceRef]){
                    workItemsSourceKey
                    workItemsSourceName
                    workTrackingIntegrationType
                }
            } 
        """
        result = client.execute(query, variable_values=dict(key=work_item_key))
        assert 'data' in result
        work_item = result['data']['workItem']
        assert work_item['workItemsSourceKey']
        assert work_item['workItemsSourceName']
        assert work_item['workTrackingIntegrationType']

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

            assert set(map(lambda edge: edge['node']['key'], edges)) == {f'{uuid.UUID(work_item_key)}:0',
                                                                         f'{uuid.UUID(work_item_key)}:1'}

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
                                            workTrackingIntegrationType
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
                assert node['workTrackingIntegrationType']

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
                       ('unmapped_state', None, None)
                   }

        def it_returns_current_delivery_cycle_commit_summary(self, api_work_items_import_fixture):
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

            api_helper.update_delivery_cycles(([(0, dict(property='commit_count', value=2))]))
            api_helper.update_delivery_cycles(([(0, dict(property='earliest_commit', value=datetime.utcnow()))]))
            api_helper.update_delivery_cycles(([(0, dict(property='latest_commit', value=datetime.utcnow()))]))

            client = Client(schema)
            query = """
                    query getWorkItem($key:String!) {
                        workItem(key: $key, interfaces:[WorkItemStateDetails]){
                            ... on WorkItemStateDetails {
                                workItemStateDetails {
                                    earliestCommit
                                    latestCommit
                                    commitCount
                                }
                            }
                        }
                    } 
                """
            result = client.execute(query, variable_values=dict(key=work_item_key))
            assert 'data' in result
            details = result['data']['workItem']['workItemStateDetails']
            assert details['commitCount'] == 2
            assert details['latestCommit']
            assert details['earliestCommit']

        def it_returns_current_delivery_effort_and_duration(self, api_work_items_import_fixture):
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

            api_helper.update_delivery_cycles(([(0, dict(property='commit_count', value=2))]))
            api_helper.update_delivery_cycles(
                ([(0, dict(property='earliest_commit', value=datetime.utcnow() - timedelta(days=3)))]))
            api_helper.update_delivery_cycles(([(0, dict(property='latest_commit', value=datetime.utcnow()))]))
            api_helper.update_delivery_cycles(([(0, dict(property='effort', value=2))]))

            client = Client(schema)
            query = """
                    query getWorkItem($key:String!) {
                        workItem(key: $key, interfaces:[WorkItemStateDetails]){
                            ... on WorkItemStateDetails {
                                workItemStateDetails {
                                    effort
                                    duration
                                }
                            }
                        }
                    } 
                """
            result = client.execute(query, variable_values=dict(key=work_item_key))
            assert 'data' in result
            details = result['data']['workItem']['workItemStateDetails']
            assert details['duration'] - 3.0 < 0.1
            assert details['effort'] == 2

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

            assert set(map(lambda edge: edge['node']['key'], edges)) == {f'{test_repo.key}:XXXXXX',
                                                                         f'{test_repo.key}:YYYYYY'}


class TestWorkItemInstanceImplementationCost:

    @staticmethod
    @pytest.yield_fixture
    def implementation_cost_fixture(implementation_effort_fixture):
        fixture = implementation_effort_fixture
        commits_common = fixture['commits_common']

        organization = fixture['organization']
        contributor_a = fixture['contributors'][0]
        contributor_b = fixture['contributors'][1]
        test_repo = fixture['repositories']['alpha']
        add_work_item_commits = fixture['add_work_item_commits']

        test_work_item = fixture['work_items'][0]
        commit_date = datetime.utcnow()
        test_commits = [
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=commit_date - timedelta(days=3),
                **contributor_a['as_author'],
                **contributor_a['as_committer'],
                **commits_common
            ),
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=commit_date - timedelta(days=2),
                **contributor_a['as_author'],
                **contributor_a['as_committer'],
                **commits_common
            ),
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=test_repo.id,
                commit_date=commit_date,
                **contributor_b['as_author'],
                **contributor_b['as_committer'],
                **commits_common
            )
        ]

        create_test_commits(test_commits)

        work_item_commits = [
            dict(
                organization_key=organization.key,
                work_item_key=test_work_item['key'],
                commit_key=test_commits[i]['key']
            )
            for i in range(0, 3)
        ]
        add_work_item_commits(work_item_commits)

        yield fixture

    def it_reports_effort_at_the_work_item_level(self, implementation_cost_fixture):
        fixture = implementation_cost_fixture
        test_work_item = fixture['work_items'][0]

        with db.orm_session() as session:
            work_item = WorkItem.find_by_work_item_key(session, test_work_item['key'])
            work_item.effort = 3.5

        client = Client(schema)
        query = """
                query getWorkItem($key:String!) {
                    workItem(key: $key, interfaces: [ImplementationCost]){
                            effort
                            duration
                            authorCount
                    }
                } 
        """
        result = client.execute(query, variable_values=dict(key=test_work_item['key']))
        assert 'data' in result
        work_item = result['data']['workItem']
        assert work_item['effort'] == 3.5

    def it_reports_aggregate_duration_from_all_commits_on_work_item(self, implementation_cost_fixture):
        fixture = implementation_cost_fixture
        test_work_item = fixture['work_items'][0]

        client = Client(schema)
        query = """
                query getWorkItem($key:String!) {
                    workItem(key: $key, interfaces: [ImplementationCost]){
                            effort
                            duration
                            authorCount
                    }
                } 
        """
        result = client.execute(query, variable_values=dict(key=test_work_item['key']))
        assert 'data' in result
        work_item = result['data']['workItem']
        assert work_item['duration'] == 3

    def it_reports_aggregate_author_count_across_all_commits(self, implementation_cost_fixture):
        fixture = implementation_cost_fixture
        test_work_item = fixture['work_items'][0]

        client = Client(schema)
        query = """
                query getWorkItem($key:String!) {
                    workItem(key: $key, interfaces: [ImplementationCost]){
                            effort
                            duration
                            authorCount
                    }
                } 
        """
        result = client.execute(query, variable_values=dict(key=test_work_item['key']))
        assert 'data' in result
        work_item = result['data']['workItem']
        assert work_item['authorCount'] == 2

    def it_reports_results_when_work_item_has_no_commits(self, implementation_cost_fixture):
        fixture = implementation_cost_fixture
        test_work_item = fixture['work_items'][1]

        client = Client(schema)
        query = """
                query getWorkItem($key:String!) {
                    workItem(key: $key, interfaces: [ImplementationCost]){
                            effort
                            duration
                            authorCount
                    }
                } 
        """
        result = client.execute(query, variable_values=dict(key=test_work_item['key']))
        assert 'data' in result
        work_item = result['data']['workItem']
        assert not work_item['authorCount']
        assert not work_item['duration']
        assert not work_item['effort']


class TestWorkItemInstancePullRequests:

    @pytest.yield_fixture()
    def setup(self, api_pull_requests_import_fixture):
        organization, project, repositories, work_items_source, work_items_common, pull_requests_common = api_pull_requests_import_fixture
        api_helper = PullRequestImportApiHelper(organization, repositories, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4(),
                name=f'Issue {i}',
                display_id='1000',
                state='open',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 1)
        ]

        pull_requests = [
            dict(
                repository_id=repositories['alpha'].id,
                key=uuid.uuid4(),
                source_id=f'100{i}',
                source_branch='1000',
                source_repository_id=repositories['alpha'].id,
                title="Another change. Fixes issue #1000",
                created_at=start_date,
                updated_at=start_date,
                end_date=None,
                **pull_requests_common
            )
            for i in range(0, 2)
        ]

        yield Fixture(
            project=project,
            api_helper=api_helper,
            start_date=start_date,
            work_items=work_items,
            pull_requests=pull_requests,
            repositories=repositories
        )

    class TestWorkItemPullRequestConnection:

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup
            connection_query = """
                query getWorkItemPullRequests($key:String!) {
                            workItem(key: $key){
                                pullRequests {
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
            yield Fixture(
                parent=fixture,
                query=connection_query
            )

        class TestWhenWorkItemDoesNotExist:

            def it_returns_null_work_item(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    key=fixture.work_items[0]['key']
                ))

                assert result['data']
                assert result['data']['workItem'] == None

        class TestWhenWorkItemExistsButNoPullRequest:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                api_helper.import_work_items(fixture.work_items)

                yield fixture

            def it_returns_zero_pull_requests(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    key=fixture.work_items[0]['key']
                ))

                assert result['data']
                assert len(result['data']['workItem']['pullRequests']['edges']) == 0

            class TestWhenPullRequestsNotLinked:

                @pytest.yield_fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper
                    # Import 2 PRs
                    api_helper.import_pull_requests(fixture.pull_requests, fixture.repositories['alpha'])

                    yield fixture

                def it_returns_zero_linked_pull_requests(self, setup):
                    fixture = setup

                    client = Client(schema)

                    result = client.execute(fixture.query, variable_values=dict(
                        key=fixture.work_items[0]['key']
                    ))

                    assert result['data']
                    assert len(result['data']['workItem']['pullRequests']['edges']) == 0

                class TestWhenOnePullRequestsLinked:

                    @pytest.yield_fixture()
                    def setup(self, setup):
                        fixture = setup
                        api_helper = fixture.api_helper
                        # Link 1 PR
                        api_helper.map_pull_request_to_work_item(fixture.work_items[0]['key'],
                                                                 fixture.pull_requests[0]['key'])

                        yield fixture

                    def it_returns_one_pull_request(self, setup):
                        fixture = setup

                        client = Client(schema)

                        result = client.execute(fixture.query, variable_values=dict(
                            key=fixture.work_items[0]['key']
                        ))

                        assert result['data']
                        edges = result['data']['workItem']['pullRequests']['edges']
                        assert len(edges) == 1
                        assert edges[0]['node']['key'] == str(fixture.pull_requests[0]['key'])
