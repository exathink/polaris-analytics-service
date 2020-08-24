# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from datetime import datetime

import pytest

from polaris.analytics.db.model import Organization, Repository, Project, contributors, contributor_aliases, commits, \
    WorkItemsSource, WorkItem, WorkItemDeliveryCycle
from polaris.common import db

test_organization_key = uuid.uuid4().hex
test_contributor_key = uuid.uuid4().hex
test_repositories = ['alpha', 'beta', 'gamma', 'delta']
test_projects = ['mercury', 'venus']
test_contributor_name = 'Joe Blow'
work_items_common = dict(
    name='Issue',
    is_bug=True,
    work_item_type='issue',
    url='http://foo.com',
    tags=['ares2'],
    updated_at=datetime.utcnow(),
    state='open',
    description='foo',
    source_id=str(uuid.uuid4())

)


@pytest.yield_fixture()
def org_repo_fixture(setup_schema):
    repositories = {}
    projects = {}
    with db.orm_session() as session:
        session.expire_on_commit = False
        organization = Organization(
            key=test_organization_key,
            name='test-org',
            public=False
        )

        for repo_name in test_repositories:
            repositories[repo_name] = Repository(
                key=uuid.uuid4(),
                name=repo_name
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
    db.connection().execute("delete from analytics.organizations")


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
    db.connection().execute("delete from analytics.work_item_delivery_cycles")
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
        author_contributor_name='Billy Bob',
        created_on_branch='master'
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
    source_id=str(uuid.uuid4())
)


def setup_work_items(organization, source_data, items_data, project_key=None):
    with db.orm_session() as session:
        session.expire_on_commit = False
        source = WorkItemsSource(
            key=uuid.uuid4(),
            organization_key=organization.key,
            organization_id=organization.id,
            **source_data
        )
        session.add(source)
        session.flush()

        work_items = []
        for w in items_data:
            work_item = WorkItem(**w)
            work_item.delivery_cycles.append(
                WorkItemDeliveryCycle(
                    start_seq_no=0,
                    start_date=datetime.utcnow(),
                    work_items_source_id=source.id
                )
            )
            session.add(work_item)
            work_items.append(work_item)
        session.flush()

        for w in work_items:
            w.current_delivery_cycle_id = w.delivery_cycles[0].delivery_cycle_id

        source.work_items.extend(work_items)

        if project_key is not None:
            project = Project.find_by_project_key(session, project_key)
            project.work_items_sources.append(source)
        
        return source
