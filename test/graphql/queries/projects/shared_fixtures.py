# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.utils.collections import dict_to_object
from test.fixtures.graphql import *
from polaris.analytics.db.model import Contributor

def import_commits(organization_key, repository_key, new_commits, new_contributors):
    # we need this rigmarole here because the api import method does not
    # set the num_parents value as that is usually not present when the commit summary is
    # imported. Did not want to change the api contract, so fixing it up in this setup
    # so that we can set num_parents after the fact. An alternative would have
    # been to use the graphql create_test_commits method, but then that makes this set of tests
    # that set num_parents different from rest of the traceability tests. So doing this here instead.
    api.import_new_commits(organization_key, repository_key, new_commits, new_contributors)
    with db.orm_session() as session:
        for new_commit in new_commits:
            if new_commit.get('num_parents') is not None:
                commit = Commit.find_by_commit_key(session, new_commit['key'])
                commit.num_parents = new_commit['num_parents']


def add_work_item_commits(work_items_commits):
    with db.orm_session() as session:
        for work_item_key, commit_key in work_items_commits:
            work_item = WorkItem.find_by_work_item_key(session, work_item_key)
            commit = Commit.find_by_commit_key(session, commit_key)
            work_item.commits.append(commit)

def exclude_contributors_from_analysis(contributors):
    with db.orm_session() as session:
        for contributor_summmary in contributors:
            contributor = Contributor.find_by_contributor_key(session, contributor_summmary['key'])
            contributor.exclude_from_analysis()

@pytest.yield_fixture
def project_commits_work_items_fixture(org_repo_fixture):
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
