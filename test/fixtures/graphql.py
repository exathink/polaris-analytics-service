# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from datetime import datetime

import pytest
from sqlalchemy import true, false

from polaris.analytics.db import api
from polaris.analytics.db.model import Account, Organization, Repository, Project, contributors, contributor_aliases, \
    commits, \
    WorkItemsSource, WorkItem, WorkItemStateTransition, Commit, FeatureFlag, FeatureFlagEnablement
from polaris.common import db
from polaris.utils.collections import find
from polaris.common.enums import WorkTrackingIntegrationType

test_user_key = uuid.uuid4().hex
test_account_key = uuid.uuid4().hex
test_organization_key = uuid.uuid4().hex
test_contributor_key = uuid.uuid4().hex
test_repositories = ['alpha', 'beta', 'gamma', 'delta']
test_projects = ['mercury', 'venus']
test_contributor_name = 'Joe Blow'

test_scope_key = uuid.uuid4()
enablements = [
    dict(scope="user", scope_key=test_scope_key, enabled=True),
    dict(scope="user", scope_key=uuid.uuid4(), enabled=False),
    dict(scope="account", scope_key=uuid.uuid4(), enabled=False)
]

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

    db.connection().execute("delete from analytics.projects_repositories")
    db.connection().execute("delete from analytics.commits")
    db.connection().execute("delete from analytics.repositories")
    db.connection().execute("delete from analytics.projects")
    db.connection().execute("delete from analytics.accounts_organizations")
    db.connection().execute("delete from analytics.organizations")
    db.connection().execute("delete from analytics.accounts")


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

    contributor = dict(alias_id=contributor_alias_id, contributor_id=contributor_id)
    yield organization, projects, repositories, contributor


@pytest.yield_fixture()
def cleanup():
    yield
    db.connection().execute("delete from analytics.feature_flag_enablements")
    db.connection().execute("delete from analytics.feature_flags")
    db.connection().execute("delete from analytics.work_items_source_state_map")
    db.connection().execute("delete from analytics.work_items_commits")
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
        session.add(source)
        return source

def create_project_work_items(organization, project, source_data, items_data):
    with db.orm_session() as session:
        session.expire_on_commit = False
        source = WorkItemsSource(
            key=uuid.uuid4().hex,
            organization_key=organization.key,
            organization_id=organization.id,
            project_id=project.id,
            **source_data
        )
        source.work_items.extend([
            WorkItem(**item)
            for item in items_data
        ])
        session.add(source)
        return source


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
            assert None,  f"Failed to find work item with key {work_item_key}"


def create_work_item_commits(work_item_key, commit_keys):
    with db.orm_session() as session:
        work_item = WorkItem.find_by_work_item_key(session, work_item_key)
        if work_item:
            for commit_key in commit_keys:
                if commit_key:
                    commit = Commit.find_by_commit_key(session, commit_key)
                    work_item.commits.append(commit)
                else:
                    assert None, f"Failed to find commit with key {commit_key}"
        else:
            assert None, f"Failed to find work item with key {work_item_key}"


work_items_common = dict(
    is_bug=True,
    work_item_type='issue',
    url='http://foo.com',
    tags=['ares2'],
    state='open',
    description='foo',
    source_id=str(uuid.uuid4()),
    state_type='open'
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
    yield new_key, test_commit_key, new_work_items,project


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
        session.expire_on_commit=False
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
        session.expire_on_commit=False
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
        session.expire_on_commit=False
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


@pytest.yield_fixture()
def create_feature_flag_fixture(cleanup):
    test_feature_flag_name = 'Test Feature Flag'
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create("Test Feature Flag")
        session.add(feature_flag)
    yield feature_flag, session

@pytest.yield_fixture()
def create_feature_flag_enablement_fixture(cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create("Feature1")
        feature_flag.enablements.extend([
            FeatureFlagEnablement(**item)
            for item in enablements
        ])
        session.add(feature_flag)
    yield feature_flag, session
