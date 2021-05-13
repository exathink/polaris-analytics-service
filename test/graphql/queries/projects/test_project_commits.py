# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.graphql.queries.projects.shared_fixtures import *


class TestProjectCommitsConnection:

    @pytest.yield_fixture()
    def setup(self, project_commits_work_items_fixture):
        fixture = project_commits_work_items_fixture

        project = fixture.projects['mercury']
        # setup commit window
        days = 7
        latest_commit_date = datetime.utcnow()
        earliest_commit_date = latest_commit_date - timedelta(days=days)

        new_commits = [
            dict(
                source_commit_id='a-XXXX',
                commit_date=latest_commit_date,
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-XXXX',
                commit_date=earliest_commit_date,
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            )

        ]
        # one commit from repo_a
        repository_a = project.repositories[0]
        import_commits(
            organization_key=fixture.organization.key,
            repository_key=repository_a.key,
            new_commits=[
                new_commits[0]
            ],
            new_contributors=fixture.contributors
        )
        # one commit from repo_b
        repository_b = project.repositories[1]
        import_commits(
            organization_key=fixture.organization.key,
            repository_key=repository_b.key,
            new_commits=[
                new_commits[1]
            ],
            new_contributors=fixture.contributors
        )
        yield dict_to_object(
            dict(
                parent_fixture=fixture,
                project=project,
                earliest_commit=earliest_commit_date,
                latest_commit=latest_commit_date,
                days=days,
                work_items_common=fixture.work_items_common,
                project_commits=new_commits,
                contributors=fixture.contributors,
            )
        )

    class CaseThereAreNoWorkItemsInTheProject:

        def it_returns_commits_for_all_repos_associated_with_the_project(self, setup):
            fixture = setup

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!) {
                    project(key: $key){
                        commits {
                            edges {
                                node {
                                    key
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 2

        def it_respects_the_days_parameter(self, setup):
            fixture = setup

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!, $days: Int!) {
                    project(key: $key){
                        commits (days: $days) {
                            edges {
                                node {
                                    key
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key,
                # use days 1 less than the window from the fixture
                # so we can eliminate the first commit in the window
                days=fixture.days - 1
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 1

        def it_respects_the_before_parameter(self, setup):
            fixture = setup

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!, $before: DateTime!) {
                    project(key: $key){
                        commits (before: $before) {
                            edges {
                                node {
                                    key
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key,
                # use a before date earlier than the latest commmit to eliminate the
                # the latest commit.
                # use days 1 less than the window from the fixture
                # so we can include the earliest commit in the window
                before=graphql_date_string(fixture.latest_commit - timedelta(days=fixture.days - 1))
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 1

        def it_respects_the_nospecs_only_parameter(self, setup):
            fixture = setup

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!) {
                    project(key: $key){
                        commits (nospecsOnly: true) {
                            edges {
                                node {
                                    key
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key,
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]
            # In this case all commits in the project are nospecs, so all should be returned.
            assert len(project_commits) == 2

    class CaseThereAreWorkItemsInTheProject:

        @pytest.yield_fixture()
        def setup_case(self, setup):
            fixture = setup
            parent_fixture = fixture.parent_fixture
            project = fixture.project
            # create work items and add them to the project
            start_date = datetime.utcnow()
            project_work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    state_type='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **parent_fixture.work_items_common
                )
                for i in range(0, 3)
            ]
            create_project_work_items(
                parent_fixture.organization,
                project,
                source_data=dict(
                    integration_type='github',
                    commit_mapping_scope='repository',
                    commit_mapping_scope_key=project.repositories[0].key,
                    **work_items_source_common
                ),
                items_data=project_work_items
            )

            non_project_work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    state_type='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **parent_fixture.work_items_common
                )
                for i in range(0, 3)
            ]

            create_work_items(
                parent_fixture.organization,
                source_data=dict(
                    integration_type='github',
                    commit_mapping_scope='repository',
                    commit_mapping_scope_key=project.repositories[0].key,
                    **work_items_source_common
                ),
                items_data=non_project_work_items
            )

            yield dict_to_object(
                dict(
                    parent_fixture=fixture,
                    project=project,
                    project_work_items=project_work_items,
                    non_project_work_items=non_project_work_items,
                    project_commits=fixture.project_commits,
                    contributors=fixture.contributors
                )
            )

        def it_returns_commits_with_associated_work_items_in_the_project(self, setup_case):
            fixture = setup_case

            # In this case we are associating one of the work items in the project with each of the
            # existing project commits. Thus all these commits should be returned
            create_work_item_commits(
                fixture.project_work_items[0]['key'],
                [commit['key'] for commit in fixture.project_commits]
            )

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!) {
                    project(key: $key){
                        commits {
                            edges {
                                node {
                                    key
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 2

        def it_returns_work_item_summaries_for_specs_in_the_project(self, setup_case):
            fixture = setup_case

            # In this case we are associating one of the work items in the project with each of the
            # existing project commits. Thus all these commits should be returned
            create_work_item_commits(
                fixture.project_work_items[0]['key'],
                [commit['key'] for commit in fixture.project_commits]
            )

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!) {
                    project(key: $key){
                        commits(interfaces: [WorkItemsSummaries]) {
                            edges {
                                node {
                                    key
                                    workItemsSummaries {
                                        key
                                        name
                                        workItemType
                                        displayId
                                        url
                                        state
                                        stateType
                                    }
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 2
            for commit in project_commits:
                assert len(commit['workItemsSummaries']) == 1
                summary = commit['workItemsSummaries'][0]
                assert summary['name']
                assert summary['workItemType']
                assert summary['displayId']
                assert summary['url']
                assert summary['state']
                assert summary['stateType']

        def it_returns_multiple_work_item_summaries_when_a_commit_is_mapped_to_multiple_specs(self, setup_case):
            fixture = setup_case

            # In this case we are associating two work items in the project with each of the
            # existing project commits. Thus all these commits should be returned
            create_work_item_commits(
                fixture.project_work_items[0]['key'],
                [commit['key'] for commit in fixture.project_commits]
            )

            create_work_item_commits(
                fixture.project_work_items[1]['key'],
                [commit['key'] for commit in fixture.project_commits]
            )

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!) {
                    project(key: $key){
                        commits(interfaces: [WorkItemsSummaries]) {
                            edges {
                                node {
                                    key
                                    workItemsSummaries {
                                        key
                                        name
                                        workItemType
                                        displayId
                                        url
                                        state
                                        stateType
                                    }
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 2
            for commit in project_commits:
                assert len(commit['workItemsSummaries']) == 2





        def it_returns_associated_commits_and_no_spec_commits(self, setup_case):
            fixture = setup_case

            # In this case we are associating one of the work items in the project with exactly one of the
            # existing project commits. The other one is nospec commit for the project.
            # In this case too both commits should be returned.
            create_work_item_commits(
                fixture.project_work_items[0]['key'],
                [fixture.project_commits[0]['key']]
            )

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!) {
                    project(key: $key){
                        commits {
                            edges {
                                node {
                                    key
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 2


        def it_excludes_associated_commits_that_are_not_part_of_the_project(self, setup_case):
            fixture = setup_case
            parent_fixture = fixture.parent_fixture

            # In this case we are associating one of the work items in the project with exactly one of the
            # existing project commits.
            create_work_item_commits(
                fixture.project_work_items[0]['key'],
                [fixture.project_commits[0]['key']]
            )

            # The other commit we are associating with a non-project work item
            # So this commit is not an associated commit nor is a no-spec commit for the project
            # so this should be excluded.
            create_work_item_commits(
                fixture.non_project_work_items[0]['key'],
                [fixture.project_commits[1]['key']]
            )

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!) {
                    project(key: $key){
                        commits {
                            edges {
                                node {
                                    key
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 1

        def it_respects_the_nospecs_only_parameter(self, setup_case):
            fixture = setup_case

            # In this case we are associating one of the work items in the project with exactly one of the
            # existing project commits. The other one is nospec commit for the project.
            # Since we are requesting nospecs_only, only this second commit should be returned
            create_work_item_commits(
                fixture.project_work_items[0]['key'],
                [fixture.project_commits[0]['key']]
            )

            client = Client(schema)
            query = """
                query getProjectCommits($key: String!) {
                    project(key: $key){
                        commits (nospecsOnly: true){
                            edges {
                                node {
                                    key
                                }
                            }
                        }
                    }
                }
            """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 1

        def it_respects_the_days_parameter(self, setup_case):
            fixture = setup_case

            # In this case we are associating one of the work items in the project with exactly one of the
            # existing project commits. The other one is nospec commit for the project.
            # Both are in scope for the project commits, but the earliest_commit should be
            # filtered out by the days parameter
            create_work_item_commits(
                fixture.project_work_items[0]['key'],
                [fixture.project_commits[0]['key']]
            )

            client = Client(schema)
            query = """
                            query getProjectCommits($key: String!, $days: Int!) {
                                project(key: $key){
                                    commits (days: $days) {
                                        edges {
                                            node {
                                                key
                                            }
                                        }
                                    }
                                }
                            }
                        """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key,
                # use days 1 less than the window from the fixture
                # so we can eliminate the first commit in the window
                days=fixture.parent_fixture.days - 1
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 1

        def it_respects_the_before_parameter(self, setup_case):
            fixture = setup_case
            parent_fixture= fixture.parent_fixture
            # In this case we are associating one of the work items in the project with exactly one of the
            # existing project commits. The other one is nospec commit for the project. Both are in scope for the
            # project commits, but the latest_commit should be filtered out by the before parameter
            create_work_item_commits(
                fixture.project_work_items[0]['key'],
                [fixture.project_commits[0]['key']]
            )

            client = Client(schema)
            query = """
                            query getProjectCommits($key: String!, $before: DateTime!) {
                                project(key: $key){
                                    commits (before: $before) {
                                        edges {
                                            node {
                                                key
                                            }
                                        }
                                    }
                                }
                            }
                        """
            result = client.execute(query, variable_values=dict(
                key=fixture.project.key,
                # use a before date earlier than the latest commmit to eliminate the
                # the latest commit.
                # use days 1 less than the window from the fixture
                # so we can include the earliest commit in the window
                before=graphql_date_string(parent_fixture.latest_commit - timedelta(days=parent_fixture.days - 1))
            ))

            assert result['data']
            project_commits = [
                edge['node']
                for edge in result['data']['project']['commits']['edges']
            ]

            assert len(project_commits) == 1


        class CaseWhenThereAreExcludedContributors:

            @pytest.yield_fixture()
            def setup_case(self, setup_case):
                fixture = setup_case

                contributors = fixture.parent_fixture.contributors

                exclude_contributors_from_analysis(contributors)

                yield fixture

            def it_excludes_commits_for_contributors_excluded_from_analysis(self, setup_case):
                fixture = setup_case

                client = Client(schema)
                query = """
                    query getProjectCommits($key: String!) {
                        project(key: $key){
                            commits {
                                edges {
                                    node {
                                        key
                                    }
                                }
                            }
                        }
                    }
                """
                result = client.execute(query, variable_values=dict(
                    key=fixture.project.key
                ))

                assert result['data']
                project_commits = [
                    edge['node']
                    for edge in result['data']['project']['commits']['edges']
                ]
                # we just excluded the only contributor in the setup so no commits should be returned
                assert len(project_commits) == 0
