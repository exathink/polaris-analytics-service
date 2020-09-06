# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client
from datetime import timedelta
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from polaris.analytics.db.enums import WorkItemsStateType
from test.fixtures.graphql import WorkItemImportApiHelper
from polaris.utils.collections import dict_select, dict_to_object


@pytest.yield_fixture
def api_project_traceability_test_fixture(org_repo_fixture):
    organization, projects, repositories = org_repo_fixture

    with db.orm_session() as session:
        session.add(organization)
        for project in organization.projects:
            work_items_source = WorkItemsSource(
                key=uuid.uuid4(),
                organization_key=organization.key,
                integration_type='jira',
                commit_mapping_scope='repository',
                commit_mapping_scope_key=None,
                organization_id=organization.id,
                **work_items_source_common
            )
            work_items_source.init_state_map(
                [
                    dict(state='backlog', state_type=WorkItemsStateType.backlog.value),
                    dict(state='upnext', state_type=WorkItemsStateType.open.value),
                    dict(state='doing', state_type=WorkItemsStateType.wip.value),
                    dict(state='done', state_type=WorkItemsStateType.complete.value),
                    dict(state='closed', state_type=WorkItemsStateType.closed.value),
                ]
            )
            project.work_items_sources.append(work_items_source)

    contributor_key = uuid.uuid4().hex

    yield dict_to_object(
        dict(
            organization=organization,
            projects=projects,
            repositories=repositories,
            commit_common_fields=dict(
                commit_date_tz_offset=0,
                committer_alias_key=contributor_key,
                author_date=datetime.utcnow(),
                author_date_tz_offset=0,
                author_alias_key=contributor_key,
                created_at=datetime.utcnow(),
                commit_message='a change'

            ),
            work_items_common=dict(
                is_bug=True,
                work_item_type='issue',
                url='http://foo.com',
                tags=['ares2'],
                description='foo',
                source_id=str(uuid.uuid4()),
                is_epic=False,
            ),
            contributors=[
                dict(
                    name='Joe Blow',
                    key=contributor_key,
                    alias='joe@blow.com'
                )
            ]
        )
    )

    db.connection().execute("delete  from analytics.work_items_commits")
    db.connection().execute("delete  from analytics.work_item_state_transitions")
    db.connection().execute("delete  from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete  from analytics.work_item_delivery_cycles")
    db.connection().execute("delete  from analytics.work_items")
    db.connection().execute("delete  from analytics.work_items_source_state_map")
    db.connection().execute("delete  from analytics.work_items_sources")
    db.connection().execute("delete  from analytics.commits")
    db.connection().execute("delete  from analytics.contributor_aliases")
    db.connection().execute("delete  from analytics.contributors")


def import_commits(organization_key, repository_key, new_commits, new_contributors):
    # we need this rigmarole here because the api import method does not
    # set the num_parents value as that is usually not present when the commit summary is
    # imported. Did not want to change the api contract, so fixing it up in this setup
    # so that we can set num_parents after the fact. An alternative would have
    # been to use the graphql create_test_commmits method, but then that makes this set of tests
    # that set num_parents different from rest of the traceability tests. So doing this here instead.
    api.import_new_commits(organization_key, repository_key, new_commits, new_contributors)
    with db.orm_session() as session:
        for new_commit in new_commits:
            if new_commit['num_parents'] is not None:
                commit = Commit.find_by_commit_key(session, new_commit['key'])
                commit.num_parents = new_commit['num_parents']



def add_work_item_commits(work_items_commits):
    with db.orm_session() as session:
        for work_item_key, commit_key in work_items_commits:
            work_item = WorkItem.find_by_work_item_key(session, work_item_key)
            commit = Commit.find_by_commit_key(session, commit_key)
            work_item.commits.append(commit)


class TestProjectTraceabilityTrends:
    project_traceability_query = """
        query getProjectTraceabilityTrends(
            $project_key:String!, 
            $days: Int!, 
            $window: Int!,
            $sample: Int,
            $exclude_merges: Boolean,
        ) {
            project(
                key: $project_key, 
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



    # Base cases: Test that nothing is dropped when there are no work items or commits. We should
    # see a series with one point for each date in the interval with zero values for all the metrics.
    def it_reports_all_zeros_when_there_are_no_work_items_or_commits_in_the_project_and_a_single_sample_in_window(self,
                                                                                                                  api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=30,
            sample=30
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
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
                                                                                                                   api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=30,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
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
                                                                                                         api_project_traceability_test_fixture
                                                                                                         ):
        fixture = api_project_traceability_test_fixture

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

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
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
                   )
               ]

    def it_reports_commit_and_nospec_counts_when_there_are_commits_that_are_not_associated_with_any_work_items(self, api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

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

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
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
                   )
               ]

    def it_respects_the_exclude_merges_flag_when_there_are_commits_that_are_not_associated_with_any_work_items(self, api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

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

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7,
            exclude_merges=True
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
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
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   )
               ]

    def it_reports_traceability_and_spec_count_when_there_are_commits_associated_with_work_items(self,
                                                                                                 api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

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
            repository_key=fixture.projects['mercury'].repositories[0].key,
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

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
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
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.5,
                       specCount=1,
                       nospecCount=1,
                       totalCommits=2
                   ),
                   dict(
                       traceability=1.0,
                       specCount=1,
                       nospecCount=0,
                       totalCommits=1
                   )
               ]

    def it_respects_exclude_merges_when_there_are_merge_commits_associated_with_work_items(self,
                                                                                    api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

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
            repository_key=fixture.projects['mercury'].repositories[0].key,
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

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7,
            exclude_merges=True
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
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
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   )
               ]

    def it_reports_traceability_and_spec_counts_correctly_when_repositories_are_shared_across_projects(self,
                                                                                                 api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

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

        # the default fixture sets everything to is_bug=True so we flip to set up this test.
        work_items[0]['is_bug'] = False
        work_items[1]['is_bug'] = False

        api_helper.import_work_items(work_items)

        add_work_item_commits([(work_items[0]['key'], new_commits[0]['key'])])

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']

        # the first set of assertions tests the metrics for mercury, which should not have changed in
        # from the last test.

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
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.5,
                       specCount=1,
                       nospecCount=1,
                       totalCommits=2
                   ),
                   dict(
                       traceability=1.0,
                       specCount=1,
                       nospecCount=0,
                       totalCommits=1
                   )
               ]

        # now we test the metrics for venus. which shares commits with mercury, but does not have  work items, so its
        # spec count should be zero and spec_count + non_spec_count should be < total commit count

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['venus'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']

        # the first set of assertions tests the metrics for mercury, which should not have changed in
        # from the last test.

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
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=2
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=1
                   )
               ]
