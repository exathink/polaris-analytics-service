# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from datetime import datetime

import pytest

from polaris.analytics.db.model import Account, Organization, Repository, Project, contributors, contributor_aliases, commits, \
    WorkItemsSource, WorkItem, WorkItemStateTransition, Commit
from polaris.common import db
from polaris.utils.collections import find

test_account_key = uuid.uuid4().hex
test_organization_key = uuid.uuid4().hex
test_contributor_key = uuid.uuid4().hex
test_repositories = ['alpha', 'beta', 'gamma', 'delta']
test_projects = ['mercury', 'venus']
test_contributor_name = 'Joe Blow'


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
            name='test-account'
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
                url=f'git@github.com/{repo_name}'
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
