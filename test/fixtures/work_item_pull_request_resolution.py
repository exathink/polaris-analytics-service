# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


import uuid
import pytest

from polaris.analytics.db.model import Organization, Repository, Project, pull_requests, \
    WorkItemsSource, WorkItem
from polaris.common import db

from datetime import datetime

test_organization_key = uuid.uuid4().hex
test_repositories = ['alpha', 'beta', 'gamma', 'delta']
test_projects = ['mercury', 'venus']
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


def get_date(str_date):
    return datetime.strptime(str_date, "%Y-%m-%d")


def pull_requests_common_fields():
    return dict(
        state="opened",
        updated_at=get_date("2020-06-23"),
        merge_status="can_be_merged",
        merged_at=get_date("2020-06-11"),
        source_branch='test',
        target_branch="master",
        description='',
        web_url="https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
    )


@pytest.yield_fixture()
def cleanup():
    yield
    db.connection().execute("delete from analytics.work_items_pull_requests")
    db.connection().execute("delete from analytics.pull_requests")

    db.connection().execute("delete from analytics.work_items_commits")
    db.connection().execute("delete from analytics.work_items")
    db.connection().execute("delete from analytics.work_items_sources")

    db.connection().execute("delete from analytics.commits")
    db.connection().execute("delete from analytics.contributor_aliases")
    db.connection().execute("delete from analytics.contributors")


def create_test_pull_requests(test_pull_requests):
    with db.create_session() as session:
        session.connection.execute(
            pull_requests.insert(test_pull_requests)
        )


@pytest.yield_fixture()
def pull_requests_fixture(org_repo_fixture, cleanup):
    organization, projects, repositories = org_repo_fixture
    yield organization, projects, repositories


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
        source.work_items.extend([
            WorkItem(**item)
            for item in items_data
        ])
        session.add(source)
        if project_key is not None:
            project = Project.find_by_project_key(session, project_key)
            project.work_items_sources.append(source)

        return source
