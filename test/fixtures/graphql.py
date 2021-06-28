# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import true, false, and_
from sqlalchemy.dialects.postgresql import insert

from polaris.analytics.db.enums import WorkItemsStateType

from polaris.analytics.db import api
from polaris.analytics.db.model import Account, Organization, Repository, Project, contributors, contributor_aliases, \
    commits, work_items_commits as work_items_commits_table, \
    WorkItemsSource, WorkItem, WorkItemStateTransition, Commit, work_item_delivery_cycles, \
    WorkItemDeliveryCycle, PullRequest, work_items_pull_requests, pull_requests, repositories_contributor_aliases, \
    work_item_delivery_cycle_contributors
from polaris.common import db
from polaris.utils.collections import find, Fixture
from polaris.common.enums import WorkTrackingIntegrationType
from polaris.auth.db.model import User

test_user_key = uuid.uuid4().hex
test_account_key = uuid.uuid4().hex
test_organization_key = uuid.uuid4().hex
test_contributor_key = uuid.uuid4().hex
test_repositories = ['alpha', 'beta', 'gamma', 'delta']
test_projects = ['mercury', 'venus']
test_contributor_name = 'Joe Blow'


def graphql_date(date):
    try:
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(date, "%Y-%m-%d")


def graphql_date_string(date):
    return datetime.strftime(date, "%Y-%m-%dT%H:%M:%S.%f")


def getRepository(name):
    with db.orm_session() as session:
        organization = Organization.find_by_organization_key(session, test_organization_key)
        return find(organization.repositories, lambda repository: repository.name == name)


@pytest.yield_fixture()
def org_repo_fixture(setup_schema):
    repositories = {}
    projects = {}
    with db.orm_session() as session:
        session.expire_on_commit = False
        account = Account(
            key=test_account_key,
            name='test-account',
            owner_key=test_user_key,
            created=datetime.utcnow(),
            updated=datetime.utcnow()
        )
        organization = Organization(
            key=test_organization_key,
            name='test-org',
            public=False
        )
        account.organizations.append(organization)

        for repo_name in test_repositories:
            repositories[repo_name] = Repository(
                key=uuid.uuid4().hex,
                name=repo_name,
                url=f'git@github.com/{repo_name}',
                commit_count=2,
                earliest_commit=get_date("2020-01-10"),
                latest_commit=get_date("2020-02-05"),
                integration_type='github'
            )
            organization.repositories.append(repositories[repo_name])

        for project_name in test_projects:
            projects[project_name] = Project(
                key=uuid.uuid4().hex,
                name=project_name
            )
            organization.projects.append(projects[project_name])

        projects['mercury'].repositories.extend([
            repositories['alpha'], repositories['beta']
        ])

        projects['venus'].repositories.extend([
            repositories['alpha'], repositories['gamma']
        ])

        session.add(organization)
        session.flush()

    yield organization, projects, repositories

    db.connection().execute("delete from analytics.source_files")
    db.connection().execute("delete from analytics.projects_repositories")
    db.connection().execute("delete from analytics.work_items_commits")
    db.connection().execute("delete from analytics.work_items_teams")
    db.connection().execute("delete from analytics.work_items")

    db.connection().execute("delete from analytics.work_items_sources")
    db.connection().execute("delete from analytics.commits")
    db.connection().execute("delete from analytics.repositories")
    db.connection().execute("delete from analytics.projects")
    db.connection().execute("delete from analytics.accounts_organizations")
    db.connection().execute("delete from analytics.contributors_teams")
    db.connection().execute("delete from analytics.teams")
    db.connection().execute("delete from analytics.organizations")
    db.connection().execute("delete from analytics.accounts")


@pytest.yield_fixture()
def user_fixture(setup_auth_schema, org_repo_fixture):
    organization, projects, repositories = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        user = User(
            key=test_user_key,
            user_name='Test User 1',
            email='testuser@exathink.com',
            first_name='Test',
            last_name='User',
            account_key=test_account_key
        )
        session.add(user)
    yield organization, projects, repositories, user

    db.connection().execute("delete from auth.users")


@pytest.yield_fixture()
def commits_fixture(org_repo_fixture, cleanup):
    organization, projects, repositories = org_repo_fixture

    with db.create_session() as session:
        contributor_id = session.connection.execute(
            contributors.insert(
                dict(
                    key=test_contributor_key,
                    name=test_contributor_name
                )
            )
        ).inserted_primary_key[0]

        contributor_alias_id = session.connection.execute(
            contributor_aliases.insert(
                dict(
                    source_alias='joe@blow.com',
                    key=test_contributor_key,
                    name=test_contributor_name,
                    contributor_id=contributor_id,
                    source='vcs'
                )
            )
        ).inserted_primary_key[0]

        session.connection.execute(
            repositories_contributor_aliases.insert(
                dict(
                    repository_id=repositories['alpha'].id,
                    contributor_alias_id=contributor_alias_id,
                    earliest_commit=get_date("2018-12-03"),
                    latest_commit=get_date("2021-02-03"),
                    commit_count=200,
                    contributor_id=contributor_id,
                    robot=False
                )
            )
        )

    contributor = dict(alias_id=contributor_alias_id, contributor_id=contributor_id)
    yield organization, projects, repositories, contributor


@pytest.yield_fixture()
def cleanup():
    yield
    db.connection().execute("delete from analytics.work_items_commits")
    db.connection().execute("delete from analytics.work_item_source_file_changes")
    db.connection().execute("delete from analytics.work_item_delivery_cycle_contributors")
    db.connection().execute("delete from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete from analytics.work_item_delivery_cycles")
    db.connection().execute("delete from analytics.feature_flag_enablements")
    db.connection().execute("delete from analytics.feature_flags")
    db.connection().execute("delete from analytics.work_items_source_state_map")

    db.connection().execute("delete from analytics.work_item_state_transitions")
    db.connection().execute("delete from analytics.work_items")
    db.connection().execute("delete from analytics.work_items_sources")

    db.connection().execute("delete from analytics.commits")
    db.connection().execute("delete from analytics.contributor_aliases")
    db.connection().execute("delete from analytics.contributors")


def commits_common_fields(commits_fixture):
    _, _, _, contributor = commits_fixture

    contributor_alias = contributor['alias_id']
    return dict(
        commit_date=datetime.utcnow(),
        commit_date_tz_offset=0,
        committer_contributor_alias_id=contributor_alias,
        committer_contributor_key=test_contributor_key,
        committer_contributor_name='Joe Blow',
        author_date_tz_offset=0,
        author_contributor_alias_id=contributor_alias,
        author_contributor_key=test_contributor_key,
        author_contributor_name='Billy Bob'
    )


def commit_summary_common_fields(commits_fixture):
    _, _, _, contributor = commits_fixture

    contributor_alias = contributor['alias_id']
    return dict(
        commit_date_tz_offset=0,
        committer_contributor_alias_id=contributor_alias,
        committer_contributor_key=test_contributor_key,
        committer_contributor_name='Joe Blow',
        author_date_tz_offset=0,
        author_contributor_alias_id=contributor_alias,
        author_contributor_key=uuid.uuid4().hex,
        author_contributor_name='Billy Bob'
    )


def create_test_commits(test_commits):
    with db.create_session() as session:
        session.connection.execute(
            commits.insert(test_commits)
        )


def get_date(str_date):
    return datetime.strptime(str_date, "%Y-%m-%d")


work_items_source_common = dict(
    name='foo',
    work_items_source_type='repository_issues',
    source_id=str(uuid.uuid4())
)


def create_work_items(organization, source_data, items_data):
    with db.orm_session() as session:
        session.expire_on_commit = False
        source = WorkItemsSource(
            key=uuid.uuid4().hex,
            organization_key=organization.key,
            organization_id=organization.id,
            **source_data
        )
        source.work_items.extend([
            WorkItem(**item)
            for item in items_data
        ])
        source.init_state_map()
        session.add(source)
        return source


def create_work_items_with_default_delivery_cycle(organization, source_data, items_data):
    with db.orm_session() as session:
        session.expire_on_commit = False
        source = WorkItemsSource(
            key=uuid.uuid4().hex,
            organization_key=organization.key,
            organization_id=organization.id,
            **source_data
        )
        source.init_state_map()
        session.add(source)
        session.flush()
        for item in items_data:
            work_item = WorkItem(**item)
            work_item.delivery_cycles.append(
                WorkItemDeliveryCycle(
                    start_seq_no=0,
                    start_date=work_item.created_at or datetime.utcnow(),
                    work_items_source_id=source.id
                )
            )
            source.work_items.append(work_item)

        session.flush()

        for work_item in source.work_items:
            work_item.current_delivery_cycle_id = work_item.delivery_cycles[0].delivery_cycle_id

        return source


def create_project_work_items(organization, project, source_data, items_data):
    return create_work_items_with_default_delivery_cycle(
        organization,
        source_data=dict(
            project_id=project.id,
            **source_data
        ),
        items_data=items_data
    )


def create_transitions(work_item_key, transitions):
    with db.orm_session() as session:
        work_item = WorkItem.find_by_work_item_key(session, work_item_key)
        if work_item:
            work_item.state_transitions.extend([
                WorkItemStateTransition(
                    **transition
                )
                for transition in transitions
            ]
            )
        else:
            assert None, f"Failed to find work item with key {work_item_key}"


def create_work_item_commits(work_item_key, commit_keys):
    with db.orm_session() as session:
        work_item = WorkItem.find_by_work_item_key(session, work_item_key)
        if work_item:
            for commit_key in commit_keys:
                commit = Commit.find_by_commit_key(session, commit_key)
                if commit:
                    work_item.commits.append(commit)
                else:
                    assert None, f"Failed to find commit with key {commit_key}"

            session.flush()
            # we are associating delivery cycles with work item commits using the
            # commit date and the start and end dates of the delivery cycle as the
            # boundaries. In earlier iterations we were using this as the definition
            # of the delivery cycle commit relationship and this was implicitly used when
            # we computed the metrics. Now, we are explictly
            # mapping the current delivery cycle id at the time
            # when a commit is mapped to a work item as the delivery cycle id and using this
            # relationship rather than a date based comparison for determining which commmits
            # belong to a delivery cycle .

            # In order for shim existing test assumptions and fixtures, we are
            # re-creating logic of associating commit dates with delivery cycles
            # here in the test_fixture. This should yield identical results for existing tests.
            for commit_key in commit_keys:
                commit = Commit.find_by_commit_key(session, commit_key)
                if commit:
                    delivery_cycle = find(
                        work_item.delivery_cycles,
                        lambda dc:
                        # This is the old date based rule we were using
                        # to map delivery cycles to commits.
                        dc.start_date <= commit.commit_date and
                        (dc.end_date is None or commit.commit_date <= dc.end_date)
                    )
                    delivery_cycle_id = delivery_cycle.delivery_cycle_id if delivery_cycle is not None else work_item.current_delivery_cycle_id
                    session.connection().execute(
                        work_items_commits_table.update().where(
                            and_(
                                work_items_commits_table.c.work_item_id == work_item.id,
                                work_items_commits_table.c.commit_id == commit.id
                            )

                        ).values(
                            delivery_cycle_id=delivery_cycle_id
                        )
                    )
                else:
                    assert None, f"Failed to find commit with key {commit_key}"
        else:
            assert None, f"Failed to find work item with key {work_item_key}"


work_items_common = dict(
    is_bug=True,
    is_epic=False,
    parent_id=None,
    work_item_type='issue',
    url='http://foo.com',
    tags=['ares2'],
    state='open',
    description='foo',
    source_id=str(uuid.uuid4()),
    state_type='open',
    next_state_seq_no=2,
)


@pytest.yield_fixture
def work_items_fixture(commits_fixture):
    organization, _, repositories, _ = commits_fixture
    test_repo = repositories['alpha']
    new_key = uuid.uuid4()
    new_work_items = [
        dict(
            key=new_key.hex,
            name='Issue 1',
            display_id='1000',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            **work_items_common
        ),
        dict(
            key=uuid.uuid4().hex,
            name='Issue 2',
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
    test_commit_source_id = 'XXXXXX'
    test_commit_key = uuid.uuid4()
    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=test_commit_key.hex,
            source_commit_id=test_commit_source_id,
            commit_message="Another change. Fixes issue #1000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        ),
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id='YYYYYY',
            commit_message="Another change. Fixes issue #2000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
    ]
    create_test_commits(test_commits)
    create_work_item_commits(new_key, map(lambda commit: commit['key'], test_commits))
    yield new_key, test_commit_key, new_work_items


@pytest.yield_fixture
def work_items_commit_summary_fixture(commits_fixture):
    organization, _, repositories, _ = commits_fixture
    test_repo = repositories['alpha']
    new_key = uuid.uuid4()
    new_work_items = [
        dict(
            key=new_key.hex,
            name='Issue 1',
            display_id='1002',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            **work_items_common
        )

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
    test_commit_source_id = 'XXXXXX'
    test_commit_key = uuid.uuid4()
    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=test_commit_key.hex,
            source_commit_id=test_commit_source_id,
            commit_message="Another change. Fixes issue #1002",
            author_date=get_date("2018-12-03"),
            commit_date=get_date("2020-01-29"),
            **commit_summary_common_fields(commits_fixture)
        ),
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id='YYYYYY',
            commit_message="Another change. Fixes issue #1002",
            author_date=get_date("2018-12-03"),
            commit_date=get_date("2020-02-05"),
            **commit_summary_common_fields(commits_fixture)
        )
    ]
    create_test_commits(test_commits)
    create_work_item_commits(new_key, map(lambda commit: commit['key'], test_commits))
    yield new_key, test_commit_key, new_work_items


@pytest.yield_fixture
def commit_summary_fixture(commits_fixture):
    organization, _, repositories, _ = commits_fixture
    project = organization.projects[0]
    test_repo = repositories['alpha']
    work_item_key = uuid.uuid4()
    new_work_items = [
        dict(
            key=work_item_key.hex,
            name='Issue 1',
            display_id='1001',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            **work_items_common
        )
    ]
    create_project_work_items(
        organization,
        project,
        source_data=dict(
            integration_type='github',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=test_repo.key,
            **work_items_source_common
        ),
        items_data=new_work_items
    )
    test_commit_source_id = 'XXXXXX'
    test_commit_key = uuid.uuid4()
    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=test_commit_key.hex,
            source_commit_id=test_commit_source_id,
            commit_message="Another change. Fixes issue #1001",
            author_date=get_date("2018-12-03"),
            commit_date=get_date("2020-01-29"),
            **commit_summary_common_fields(commits_fixture)
        ),
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id='YYYYYY',
            commit_message="Another change. Fixes issue #1001",
            author_date=get_date("2018-12-03"),
            commit_date=get_date("2020-02-05"),
            **commit_summary_common_fields(commits_fixture)
        )
    ]
    create_test_commits(test_commits)
    create_work_item_commits(work_item_key, map(lambda commit: commit['key'], test_commits))
    yield work_item_key, test_commit_key, new_work_items, project


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


@pytest.yield_fixture
def project_fixture(commits_fixture):
    organization, _, repositories, _ = commits_fixture
    project = organization.projects[0]
    test_repo = repositories['alpha']
    new_key = uuid.uuid4()
    new_work_items = [
        dict(
            key=new_key.hex,
            name='Issue 1',
            display_id='1000',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            **work_items_common
        ),
        dict(
            key=uuid.uuid4().hex,
            name='Issue 2',
            display_id='2000',
            created_at=get_date("2018-12-03"),
            updated_at=get_date("2018-12-04"),
            **work_items_common
        ),

    ]
    create_project_work_items(
        organization,
        project,
        source_data=dict(
            integration_type='github',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=test_repo.key,
            **work_items_source_common
        ),
        items_data=new_work_items
    )
    test_commit_source_id = 'XXXXXX'
    test_commit_key = uuid.uuid4()
    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=test_commit_key.hex,
            source_commit_id=test_commit_source_id,
            commit_message="Another change. Fixes issue #1000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        ),
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id='YYYYYY',
            commit_message="Another change. Fixes issue #2000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
    ]
    create_test_commits(test_commits)
    create_work_item_commits(new_key, map(lambda commit: commit['key'], test_commits))
    yield new_key, test_commit_key, new_work_items, project


@pytest.fixture
def api_import_commits_fixture(org_repo_fixture, cleanup):
    _, _, repositories = org_repo_fixture

    commit_common_fields = dict(
        commit_date_tz_offset=0,
        committer_alias_key=test_contributor_key,
        author_date=datetime.utcnow(),
        author_date_tz_offset=0,
        author_alias_key=test_contributor_key,
        created_at=datetime.utcnow(),
        commit_message='a change'

    )

    api.import_new_commits(
        organization_key=test_organization_key,
        repository_key=repositories['alpha'].key,
        new_commits=[
            dict(
                source_commit_id='a-XXXX',
                commit_date="11/1/2019",
                key=uuid.uuid4().hex,
                **commit_common_fields
            ),
            dict(
                source_commit_id='a-YYYY',
                commit_date="11/2/2019",
                key=uuid.uuid4().hex,
                **commit_common_fields
            )
        ],
        new_contributors=[
            dict(
                name='Joe Blow',
                key=test_contributor_key,
                alias='joe@blow.com'
            )
        ]
    )

    api.import_new_commits(
        organization_key=test_organization_key,
        repository_key=repositories['gamma'].key,
        new_commits=[
            dict(
                source_commit_id='b-XXXX',
                commit_date="10/1/2019",
                key=uuid.uuid4().hex,
                **commit_common_fields
            ),
            dict(
                source_commit_id='b-YYYY',
                commit_date="11/1/2019",
                key=uuid.uuid4().hex,
                **commit_common_fields
            )
        ],
        new_contributors=[
            dict(
                name='Joe Blow',
                key=test_contributor_key,
                alias='joe@blow.com'
            )
        ]
    )


@pytest.yield_fixture()
def work_items_sources_fixture(org_repo_fixture, cleanup):
    organization, _, _ = org_repo_fixture
    new_key = uuid.uuid4()
    work_items_sources = {}
    with db.orm_session() as session:
        session.expire_on_commit = False
        work_items_sources['github'] = WorkItemsSource(
            key=new_key.hex,
            integration_type='github',
            name='Test Work Items Source 1',
            organization_key=organization.key,
            commit_mapping_scope='organization',
            commit_mapping_scope_key=organization.key,
            organization_id=organization.id,
        )
        work_items_sources['pivotal'] = WorkItemsSource(
            key=uuid.uuid4().hex,
            integration_type='pivotal_tracker',
            name='Test Work Items Source 2',
            organization_key=organization.key,
            commit_mapping_scope='organization',
            commit_mapping_scope_key=organization.key,
            organization_id=organization.id,
        )
        session.add_all(work_items_sources.values())
        work_items_sources['jira'] = WorkItemsSource(
            key=uuid.uuid4().hex,
            integration_type='jira',
            name='Test Work Items Source 3',
            organization_key=organization.key,
            commit_mapping_scope='organization',
            commit_mapping_scope_key=organization.key,
            organization_id=organization.id,
        )
        session.add_all(work_items_sources.values())
    yield new_key, work_items_sources


@pytest.yield_fixture()
def work_items_sources_work_items_fixture(commits_fixture, cleanup):
    organization, _, repositories, _ = commits_fixture
    new_key = uuid.uuid4()
    work_items_sources = {}
    test_repo = repositories['alpha']
    new_work_item_key = uuid.uuid4()
    new_work_items = [
        dict(
            key=new_work_item_key.hex,
            name='Issue 5',
            display_id='1005',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            **work_items_common
        ),
        dict(
            key=uuid.uuid4().hex,
            name='Issue 6',
            display_id='1006',
            created_at=get_date("2018-12-03"),
            updated_at=get_date("2018-12-04"),
            **work_items_common
        )

    ]
    with db.orm_session() as session:
        session.expire_on_commit = False
        work_items_sources['pivotal'] = WorkItemsSource(
            key=new_key.hex,
            integration_type='pivotal_tracker',
            name='Test Work Items Source 2',
            organization_key=organization.key,
            commit_mapping_scope='organization',
            commit_mapping_scope_key=organization.key,
            organization_id=organization.id,
        )
        work_items_sources['pivotal'].work_items.extend([
            WorkItem(**item)
            for item in new_work_items
        ])
        session.add_all(work_items_sources.values())
        test_commit_source_id = 'XXXXXX'
        test_commit_key = uuid.uuid4()
        test_commits = [
            dict(
                repository_id=test_repo.id,
                key=test_commit_key.hex,
                source_commit_id=test_commit_source_id,
                commit_message="Another change. Fixes issue #1006",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            ),
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4().hex,
                source_commit_id='YYYYYY',
                commit_message="Another change. Fixes issue #1006",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ]
        create_test_commits(test_commits)
        create_work_item_commits(new_work_item_key, map(lambda commit: commit['key'], test_commits))
    yield new_key, work_items_sources


@pytest.yield_fixture()
def jira_work_items_source_work_items_states_fixture(org_repo_fixture, cleanup):
    work_items_common_fields = dict(
        is_bug=True,
        is_epic=False,
        parent_id=None,
        work_item_type='issue',
        url='http://foo.com',
        tags=['testing'],
        description='foo',
        source_id=str(uuid.uuid4()),
    )
    organization, _, _ = org_repo_fixture
    jira_source_key = uuid.uuid4()
    work_items_sources = {}

    new_work_items = [
        dict(
            key=uuid.uuid4().hex,
            name='Issue 5',
            display_id='1005',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            state='backlog',
            state_type=None,
            **work_items_common_fields
        ),
        dict(
            key=uuid.uuid4().hex,
            name='Issue 6',
            display_id='1006',
            created_at=get_date("2018-12-03"),
            updated_at=get_date("2018-12-04"),
            state='closed',
            state_type=None,
            **work_items_common_fields
        ),
    ]
    with db.orm_session() as session:
        session.expire_on_commit = False
        work_items_sources['jira'] = WorkItemsSource(
            key=jira_source_key.hex,
            integration_type=WorkTrackingIntegrationType.jira.value,
            name='Test Work Items Distinct State',
            organization_key=organization.key,
            commit_mapping_scope='organization',
            commit_mapping_scope_key=organization.key,
            organization_id=organization.id,
        )

        work_items_sources['jira'].work_items.extend([
            WorkItem(**item)
            for item in new_work_items
        ])
        work_items_sources['jira'].init_state_map()
        session.add_all(work_items_sources.values())
    yield jira_source_key, work_items_sources


@pytest.yield_fixture()
def github_work_items_source_work_items_states_fixture(org_repo_fixture, cleanup):
    work_items_common_fields = dict(
        is_bug=True,
        is_epic=False,
        parent_id=None,
        work_item_type='issue',
        url='http://foo.com',
        tags=['testing'],
        description='foo',
        source_id=str(uuid.uuid4()),
    )
    organization, _, _ = org_repo_fixture
    github_source_key = uuid.uuid4()
    work_items_sources = {}

    new_work_items = [
        dict(
            key=uuid.uuid4().hex,
            name='Issue 5',
            display_id='1005',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            state='backlog',
            state_type=None,
            **work_items_common_fields
        ),
        dict(
            key=uuid.uuid4().hex,
            name='Issue 6',
            display_id='1006',
            created_at=get_date("2018-12-03"),
            updated_at=get_date("2018-12-04"),
            state='created',
            state_type=None,
            **work_items_common_fields
        ),
    ]
    with db.orm_session() as session:
        session.expire_on_commit = False
        work_items_sources['github'] = WorkItemsSource(
            key=github_source_key.hex,
            integration_type=WorkTrackingIntegrationType.github.value,
            name='Test Work Items Distinct State',
            organization_key=organization.key,
            commit_mapping_scope='organization',
            commit_mapping_scope_key=organization.key,
            organization_id=organization.id,
        )

        work_items_sources['github'].work_items.extend([
            WorkItem(**item)
            for item in new_work_items
        ])
        work_items_sources['github'].init_state_map()
        session.add_all(work_items_sources.values())
    yield github_source_key, work_items_sources


@pytest.yield_fixture()
def pivotal_work_items_source_work_items_states_fixture(org_repo_fixture, cleanup):
    work_items_common_fields = dict(
        is_bug=True,
        is_epic=False,
        parent_id=None,
        work_item_type='issue',
        url='http://foo.com',
        tags=['testing'],
        description='foo',
        source_id=str(uuid.uuid4()),
    )
    organization, _, _ = org_repo_fixture
    pivotal_source_key = uuid.uuid4()
    work_items_sources = {}

    new_work_items = [
        dict(
            key=uuid.uuid4().hex,
            name='Issue 5',
            display_id='1005',
            created_at=get_date("2018-12-02"),
            updated_at=get_date("2018-12-03"),
            state='backlog',
            state_type=None,
            **work_items_common_fields
        ),
        dict(
            key=uuid.uuid4().hex,
            name='Issue 6',
            display_id='1006',
            created_at=get_date("2018-12-03"),
            updated_at=get_date("2018-12-04"),
            state='created',
            state_type=None,
            **work_items_common_fields
        ),
        dict(
            key=uuid.uuid4().hex,
            name='Issue 7',
            display_id='1007',
            created_at=get_date("2018-12-04"),
            updated_at=get_date("2018-12-05"),
            state='unscheduled',
            state_type=None,
            **work_items_common_fields
        ),

    ]
    with db.orm_session() as session:
        session.expire_on_commit = False
        work_items_sources['pivotal'] = WorkItemsSource(
            key=pivotal_source_key.hex,
            integration_type=WorkTrackingIntegrationType.pivotal.value,
            name='Test Work Items Distinct State',
            organization_key=organization.key,
            commit_mapping_scope='organization',
            commit_mapping_scope_key=organization.key,
            organization_id=organization.id,
        )

        work_items_sources['pivotal'].work_items.extend([
            WorkItem(**item)
            for item in new_work_items
        ])
        work_items_sources['pivotal'].init_state_map()
        session.add_all(work_items_sources.values())
    yield pivotal_source_key, work_items_sources


@pytest.yield_fixture
def api_work_items_import_fixture(org_repo_fixture):
    organization, projects, _ = org_repo_fixture

    project = projects['mercury']
    work_items_source = WorkItemsSource(
        key=uuid.uuid4(),
        organization_key=organization.key,
        integration_type='jira',
        commit_mapping_scope='repository',
        commit_mapping_scope_key=None,
        project_id=project.id,
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

    with db.orm_session() as session:
        session.add(organization)
        organization.work_items_sources.append(work_items_source)

    work_items_common = dict(
        is_bug=True,
        is_epic=False,
        parent_id=None,
        work_item_type='issue',
        url='http://foo.com',
        tags=['ares2'],
        description='foo',
        source_id=str(uuid.uuid4()),
    )

    yield organization, project, work_items_source, work_items_common

    db.connection().execute("delete  from analytics.work_item_state_transitions")
    db.connection().execute("delete  from analytics.work_item_delivery_cycle_contributors")
    db.connection().execute("delete  from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete  from analytics.work_item_delivery_cycles")
    db.connection().execute("delete  from analytics.work_items")
    db.connection().execute("delete  from analytics.work_items_source_state_map")
    db.connection().execute("delete  from analytics.work_items_sources")


class WorkItemImportApiHelper:
    def __init__(self, organization, work_items_source, work_items=None):
        self.organization = organization
        self.work_items_source = work_items_source
        self.work_items = work_items

    def import_work_items(self, work_items):
        self.work_items = work_items
        api.import_new_work_items(
            organization_key=self.organization.key,
            work_item_source_key=self.work_items_source.key,
            work_item_summaries=work_items
        )

    def update_work_items(self, updates):
        for index, state, updated in updates:
            self.work_items[index]['state'] = state
            self.work_items[index]['updated_at'] = updated

        api.update_work_items(self.organization.key, self.work_items_source.key, self.work_items)

    def update_work_item_attributes(self, index, updates, join_this=None):
        with db.orm_session(join_this) as session:
            work_item = WorkItem.find_by_work_item_key(session, self.work_items[index]['key'])
            if work_item:
                for name, value in updates.items():
                    setattr(work_item, name, value)

    def update_delivery_cycles(self, updates, join_this=None):
        with db.orm_session(join_this) as session:
            for index, update in updates:
                work_item = WorkItem.find_by_work_item_key(session, self.work_items[index]['key'])
                if work_item:
                    delivery_cycle = session.query(WorkItemDeliveryCycle).filter(
                        WorkItemDeliveryCycle.delivery_cycle_id == work_item.current_delivery_cycle_id
                    ).first()
                    if delivery_cycle:
                        setattr(delivery_cycle, update['property'], update['value'])

    def update_delivery_cycle(self, index, update_dict, join_this=None):
        with db.orm_session(join_this) as session:
            work_item = WorkItem.find_by_work_item_key(session, self.work_items[index]['key'])
            if work_item:
                delivery_cycle_id = work_item.current_delivery_cycle_id
                session.connection().execute(
                    work_item_delivery_cycles.update().where(
                        work_item_delivery_cycles.c.delivery_cycle_id == delivery_cycle_id
                    ).values(
                        update_dict
                    )
                )

    def update_work_item(self, index, update_dict, join_this=None):
        with db.orm_session(join_this) as session:
            work_item = WorkItem.find_by_work_item_key(session, self.work_items[index]['key'])
            if work_item:
                for name, value in update_dict.items():
                    setattr(work_item, name, value)

    def update_delivery_cycle_contributors(self, index, update_dict, join_this=None):
        with db.orm_session(join_this) as session:
            work_item = WorkItem.find_by_work_item_key(session, self.work_items[index]['key'])
            if work_item:
                delivery_cycle_id = work_item.current_delivery_cycle_id
                # create entry in work_item_delivery_cycle_contributors
                stmt = insert(work_item_delivery_cycle_contributors).values(
                    delivery_cycle_id=delivery_cycle_id,
                    **update_dict
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        work_item_delivery_cycle_contributors.c.delivery_cycle_id,
                        work_item_delivery_cycle_contributors.c.contributor_alias_id
                    ],
                    set_=dict(
                        total_lines_as_author=stmt.excluded.total_lines_as_author,
                        total_lines_as_reviewer=stmt.excluded.total_lines_as_reviewer
                    )
                )
                session.connection().execute(stmt)


@pytest.yield_fixture
def api_pull_requests_import_fixture(org_repo_fixture):
    organization, projects, repositories = org_repo_fixture

    project = projects['mercury']
    work_items_source = WorkItemsSource(
        key=uuid.uuid4(),
        organization_key=organization.key,
        integration_type='jira',
        commit_mapping_scope='repository',
        commit_mapping_scope_key=repositories['alpha'].key,
        project_id=project.id,
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

    with db.orm_session() as session:
        session.add(organization)
        organization.work_items_sources.append(work_items_source)

    work_items_common = dict(
        is_bug=True,
        is_epic=False,
        parent_id=None,
        work_item_type='issue',
        url='http://foo.com',
        tags=['ares2'],
        description='foo',
        source_id=str(uuid.uuid4()),
    )

    pull_requests_common = dict(
        source_state='opened',
        state="open",
        merge_status="can_be_merged",
        target_branch="master",
        description='',
        display_id='1010',
        web_url="https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
    )

    yield organization, project, repositories, work_items_source, work_items_common, pull_requests_common

    db.connection().execute("delete from analytics.work_items_pull_requests")
    db.connection().execute("delete from analytics.pull_requests")
    db.connection().execute("delete from analytics.work_item_state_transitions")
    db.connection().execute("delete from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete from analytics.work_item_delivery_cycles")
    db.connection().execute("delete from analytics.work_items")
    db.connection().execute("delete from analytics.work_items_source_state_map")
    db.connection().execute("delete from analytics.work_items_sources")


class PullRequestImportApiHelper(WorkItemImportApiHelper):

    def __init__(self, organization, repositories, work_items_source, work_items=None, pull_requests=None):
        super().__init__(organization, work_items_source, work_items)
        self.repositories = repositories
        self.pull_requests = pull_requests

    def import_pull_requests(self, pull_requests, repository):
        self.pull_requests = pull_requests
        api.import_new_pull_requests(repository.key, pull_requests)

    def map_pull_request_to_work_item(self, work_item_key, pull_request_key, join_this=None):
        with db.orm_session(join_this) as session:
            work_item = WorkItem.find_by_work_item_key(session, work_item_key)
            pull_request = PullRequest.find_by_key(session, pull_request_key)
            delivery_cycle_id = work_item.current_delivery_cycle_id
            session.connection().execute(
                work_items_pull_requests.insert().values(
                    dict(
                        work_item_id=work_item.id,
                        pull_request_id=pull_request.id,
                        delivery_cycle_id=delivery_cycle_id
                    )
                )
            )

    def update_pull_request(self, pull_request_key, update_dict, join_this=None):
        with db.orm_session(join_this) as session:
            session.connection().execute(
                pull_requests.update().where(
                    pull_requests.c.key == pull_request_key
                ).values(
                    update_dict
                )
            )
