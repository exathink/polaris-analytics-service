# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2022) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from datetime import timedelta

from polaris.analytics.db.model import commits
from polaris.common import db
from test.fixtures.graphql import create_project_work_items, work_items_source_common, generate_work_item, create_work_item_commits

def create_test_commits(test_commits, session=None):
    with db.orm_session(join_this=session) as session:
        session.connection().execute(
            commits.insert(test_commits)
        )


def create_commit_sequence(repository, contributor, end_date, start_date_offset_days, days_increment,
                           author=None, committer=None, common_commit_fields=None, session=None):
    commits = []
    start_date  = end_date - timedelta(days=start_date_offset_days)
    commit_date = start_date
    while commit_date <= end_date:
        commits.extend([
            dict(
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                repository_id=repository.id,
                commit_date=commit_date,
                **(contributor if author is None else author)['as_author'],
                **(contributor if committer is None else committer)['as_committer'],
                **(common_commit_fields if common_commit_fields is not None else {})
            )
        ])
        commit_date = commit_date + timedelta(days=days_increment)
    create_test_commits(commits, session=session)
    return commits

def create_work_items_in_project(organization, project, work_items):
    create_project_work_items(
        organization,
        project,
        source_data=dict(
            integration_type='github',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=uuid.uuid4().hex,
            **work_items_source_common
        ),
        items_data=work_items
    )


def create_commit_sequence_in_project(
        organization,
        project,
        repository,
        contributor,
        end_date,
        start_date_offset_days,
        days_increment,
        commits_common=None,
        author=None,
        committer=None,
        session=None
):
    commit_sequence = create_commit_sequence(
        repository,
        contributor=contributor,
        end_date=end_date,
        start_date_offset_days=start_date_offset_days,
        days_increment=days_increment,
        common_commit_fields=commits_common,
        author=author,
        committer=committer,
        session=session
    )
    test_work_item = generate_work_item(name='foo', display_id='1000')
    create_work_items_in_project(organization, project, [test_work_item])
    create_work_item_commits(test_work_item['key'], [commit['key'] for commit in commit_sequence])
    return commit_sequence