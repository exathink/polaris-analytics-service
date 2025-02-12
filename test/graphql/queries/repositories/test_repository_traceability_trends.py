# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import pytest
from graphene.test import Client
from datetime import timedelta
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import WorkItemImportApiHelper
from polaris.utils.collections import dict_select

from test.graphql.queries.projects.shared_fixtures import *


class TestProjectTraceabilityTrends:
    @pytest.fixture()
    def setup(self, project_commits_work_items_fixture):
        fixture = project_commits_work_items_fixture
        repository_traceability_query = """
            query getRepositoryTraceabilityTrends(
                $repository_key:String!, 
                $days: Int!, 
                $window: Int!,
                $sample: Int,
                $exclude_merges: Boolean,
            ) {
                repository(
                    key: $repository_key, 
                    interfaces: [TraceabilityTrends], 
                    traceabilityTrendsArgs: {
                        days: $days,
                        measurementWindow: $window,
                        samplingFrequency: $sample,
                        excludeMerges: $exclude_merges
                    }
    
                ) {
                    traceabilityTrends {
                        measurementDate
                        traceability
                        specCount
                        nospecCount
                        totalCommits
                    }
                }
            }
        """
        yield Fixture(
            parent=fixture,
            query=repository_traceability_query
        )

    # Base cases: Test that nothing is dropped when there are no work items or commits. We should
    # see a series with one point for each date in the interval with zero values for all the metrics.

    def it_reports_all_zeros_when_there_are_no_work_items_or_commits_in_the_project_and_a_single_sample_in_window(self,
                                                                                                                  setup):
        fixture = setup

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=30,
            sample=30
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 2
        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       # beginning of window
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       # end of window
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   )
               ]


    def it_reports_all_zeros_when_there_are_no_work_items_or_commits_in_the_project_and_multiple_samples_in_window(self,
                                                                                                                   setup):
        fixture = setup

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=30,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5
        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   )
                   for _ in range(0, 5)
               ]


    def it_reports_commit_and_nospec_counts_when_there_are_commits_in_the_project_and_zeros_for_the_rest(self,
                                                                                                         setup
                                                                                                         ):
        fixture = setup

        start_date = datetime.utcnow() - timedelta(days=10)
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.projects['mercury'].repositories[0].key,
            new_commits=[
                dict(
                    source_commit_id='a-XXXX',
                    # one commit 10 days from the end of the window
                    commit_date=start_date,
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                ),
                dict(
                    source_commit_id='a-YYYY',
                    # next commit 20 days from end of the window
                    commit_date=start_date - timedelta(days=10),
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                )
            ],
            new_contributors=fixture.contributors
        )

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=2,
                       totalCommits=2
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
               ]


    def it_excludes_robots_from_commit_and_nospec_counts_when_there_are_commits_in_the_project(self,
                                                                                                         setup
                                                                                                         ):
        fixture = setup

        start_date = datetime.utcnow() - timedelta(days=10)

        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.projects['mercury'].repositories[0].key,
            new_commits=[
                dict(
                    source_commit_id='a-XXXX',
                    # one commit 10 days from the end of the window
                    commit_date=start_date,
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                ),
                dict(
                    source_commit_id='a-YYYY',
                    # next commit 20 days from end of the window
                    commit_date=start_date - timedelta(days=10),
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                )
            ],
            new_contributors=fixture.contributors
        )

        exclude_contributors_from_analysis(fixture.contributors)

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
               ]


    def it_reports_commit_and_nospec_counts_when_there_are_commits_that_are_not_associated_with_any_work_items(self,
                                                                                                               setup):
        fixture = setup

        # same setup as last one, but we are going to add some work items but not associate them to any commits
        start_date = datetime.utcnow() - timedelta(days=10)
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.projects['mercury'].repositories[0].key,
            new_commits=[
                dict(
                    source_commit_id='a-XXXX',
                    # one commit 10 days from the end of the window
                    commit_date=start_date,
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                ),
                dict(
                    source_commit_id='a-YYYY',
                    # next commit 20 days from end of the window
                    commit_date=start_date - timedelta(days=10),
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                )
            ],
            new_contributors=fixture.contributors
        )

        api_helper = WorkItemImportApiHelper(fixture.organization, fixture.projects['mercury'].work_items_sources[0])

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # assertions here are the same as the last test, as adding work items should have no impact on the metrics

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=2,
                       totalCommits=2
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),

               ]


    def it_respects_the_exclude_merges_flag_when_there_are_commits_that_are_not_associated_with_any_work_items(self,
                                                                                                               setup):
        fixture = setup

        # same setup as last one, but we are going to add some work items but not associate them to any commits
        start_date = datetime.utcnow() - timedelta(days=10)
        import_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.projects['mercury'].repositories[0].key,
            new_commits=[
                dict(
                    source_commit_id='a-XXXX',
                    # one commit 10 days from the end of the window
                    commit_date=start_date,
                    key=uuid.uuid4().hex,
                    # make it a merge
                    num_parents=2,
                    **fixture.commit_common_fields
                ),
                dict(
                    source_commit_id='a-YYYY',
                    # next commit 20 days from end of the window
                    commit_date=start_date - timedelta(days=10),
                    key=uuid.uuid4().hex,
                    # make it a non-merge
                    num_parents=1,
                    **fixture.commit_common_fields
                )
            ],
            new_contributors=fixture.contributors
        )

        api_helper = WorkItemImportApiHelper(fixture.organization, fixture.projects['mercury'].work_items_sources[0])

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=15,
            sample=7,
            exclude_merges=True
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # assertions here are the same as the last test, as adding work items should have no impact on the metrics

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   # commit 10 days out is a non-merge and reported here and next window
                   # other commit is not reported anywhere since it is a merge.
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),

               ]


    def it_reports_traceability_and_spec_count_when_there_are_commits_associated_with_work_items(self,
                                                                                                 setup):
        fixture = setup

        # same setup as last one, but we are going to add some work items and associate some commits
        start_date = datetime.utcnow() - timedelta(days=10)
        new_commits = [
            dict(
                source_commit_id='a-XXXX',
                # one commit 10 days from the end of the window
                commit_date=start_date,
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-YYYY',
                # next commit 20 days from end of the window
                commit_date=start_date - timedelta(days=10),
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            )
        ]
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.repositories['alpha'].key,
            new_commits=new_commits,
            new_contributors=fixture.contributors
        )

        api_helper = WorkItemImportApiHelper(fixture.organization, fixture.projects['mercury'].work_items_sources[0])

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        add_work_item_commits([(work_items[0]['key'], new_commits[0]['key'])])

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # assertions here are the same as the last test, as adding work items should have no impact on the metrics

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=1.0,
                       specCount=1,
                       nospecCount=0,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.5,
                       specCount=1,
                       nospecCount=1,
                       totalCommits=2
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),

               ]

    def it_excludes_robot_contributions_from_traceability_and_spec_count_when_there_are_commits_associated_with_work_items(self,
                                                                                                 setup):
        fixture = setup

        # same setup as last one, but we are going to add some work items and associate some commits
        start_date = datetime.utcnow() - timedelta(days=10)
        new_commits = [
            dict(
                source_commit_id='a-XXXX',
                # one commit 10 days from the end of the window
                commit_date=start_date,
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-YYYY',
                # next commit 20 days from end of the window
                commit_date=start_date - timedelta(days=10),
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            )
        ]
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.repositories['alpha'].key,
            new_commits=new_commits,
            new_contributors=fixture.contributors
        )
        exclude_contributors_from_analysis(fixture.contributors)

        api_helper = WorkItemImportApiHelper(fixture.organization, fixture.projects['mercury'].work_items_sources[0])

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        add_work_item_commits([(work_items[0]['key'], new_commits[0]['key'])])

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # assertions here are the same as the last test, as adding work items should have no impact on the metrics

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),

               ]


    def it_respects_exclude_merges_when_there_are_merge_commits_associated_with_work_items(self,
                                                                                           setup):
        fixture = setup

        # same setup as last one, but we are going to add some work items and associate some commits
        start_date = datetime.utcnow() - timedelta(days=10)
        new_commits = [
            dict(
                source_commit_id='a-XXXX',
                # one commit 10 days from the end of the window
                commit_date=start_date,
                key=uuid.uuid4().hex,
                # make it a merge
                num_parents=2,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-YYYY',
                # next commit 20 days from end of the window
                commit_date=start_date - timedelta(days=10),
                num_parents=1,
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            )
        ]
        import_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.repositories['alpha'].key,
            new_commits=new_commits,
            new_contributors=fixture.contributors
        )

        api_helper = WorkItemImportApiHelper(fixture.organization, fixture.projects['mercury'].work_items_sources[0])

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)
        # associate the merge commit with the work item
        add_work_item_commits([(work_items[0]['key'], new_commits[0]['key'])])

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=15,
            sample=7,
            exclude_merges=True
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']
        # assertions here are the same as the last test, as adding work items should have no impact on the metrics

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),

               ]


    def it_reports_traceability_and_spec_counts_correctly_when_repositories_are_shared_across_projects(self,
                                                                                                       setup):
        fixture = setup

        # same setup as last one, but we are going to add some work items but not associate them to any commits
        start_date = datetime.utcnow() - timedelta(days=10)
        new_commits = [
            dict(
                source_commit_id='a-XXXX',
                # one commit 10 days from the end of the window
                commit_date=start_date,
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-YYYY',
                # next commit 20 days from end of the window
                commit_date=start_date - timedelta(days=10),
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-ZZZZ',
                # next commit 20 days from end of the window
                commit_date=start_date - timedelta(days=10),
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-ZZZX',
                # next commit 20 days from end of the window
                commit_date=start_date - timedelta(days=10),
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            )
        ]
        # alpha is a repo shared between projects mercury and venus
        alpha = fixture.repositories['alpha']
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=alpha.key,
            new_commits=new_commits,
            new_contributors=fixture.contributors
        )

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        mercury_api_helper = WorkItemImportApiHelper(fixture.organization,
                                                     fixture.projects['mercury'].work_items_sources[0])
        mercury_api_helper.import_work_items([work_items[0]])

        add_work_item_commits([(work_items[0]['key'], new_commits[0]['key'])])

        venus_api_helper = WorkItemImportApiHelper(fixture.organization,
                                                     fixture.projects['venus'].work_items_sources[0])
        venus_api_helper.import_work_items([work_items[1]])

        add_work_item_commits([(work_items[1]['key'], new_commits[1]['key'])])

        client = Client(schema)

        result = client.execute(fixture.query, variable_values=dict(
            repository_key=fixture.repositories['alpha'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['repository']['traceabilityTrends']

        # the first set of assertions tests the metrics for mercury, which should not have changed in
        # from the last test.

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=1.0,
                       specCount=1,
                       nospecCount=0,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.5,
                       specCount=2,
                       nospecCount=2,
                       totalCommits=4
                   ),
                   dict(
                       traceability=0.3333333333333333,
                       specCount=1,
                       nospecCount=2,
                       totalCommits=3
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
               ]




