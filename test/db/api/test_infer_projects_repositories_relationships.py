# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.analytics.db import aggregations

from test.fixtures.work_item_commit_resolution import *


class TestInferProjectsRepositoriesRelationships:

    def it_infers_relationships_when_work_items_commits_are_resolved(self, commits_fixture):
        organization, projects, repositories, _ = commits_fixture
        # select project and repo so that repo is NOT currently associated with project
        test_project = projects['mercury']
        test_repo = repositories['gamma']

        # create work items in a source that is attached to the project mercury and import work items into it.
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='github',
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items,
            project_key=test_project.key
        )
        # create a test commit in the repo gamma that references the work items.
        test_commit_source_id = '00001'
        test_commit_key = uuid.uuid4()
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=test_commit_key,
                source_commit_id=test_commit_source_id,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])
        # This is what the output from a resolved work items commmits operation will look like.
        resolved_work_items_commits = [
            dict(
                work_item_key=new_key,
                work_items_source_key=work_item_source.key,
                commit_key=test_commit_key,
                repository_key=test_repo.key,
            )
        ]

        result = aggregations.infer_projects_repositories_relationships(organization.key, resolved_work_items_commits)
        assert result['success']
        assert result['new_relationships'] == [
            dict(
                project_key=str(uuid.UUID(test_project.key)),
                repository_key=str(test_repo.key)
            )
        ]

        assert db.connection().execute(
            f"select count(*) from analytics.projects_repositories "
            f"where project_id={test_project.id} and repository_id={test_repo.id}"
        ).scalar() == 1

    def it_preserves_existing_relationships_when_work_items_commits_are_resolved(self, commits_fixture):
        organization, projects, repositories, _ = commits_fixture
        # select project and repo so that repo is already associated with the project
        test_project = projects['mercury']
        test_repo = repositories['alpha']

        # create work items in a source that is attached to the project mercury and import work items into it.
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='github',
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items,
            project_key=test_project.key
        )
        # create a test commit in the repo gamma that references the work items.
        test_commit_source_id = '00001'
        test_commit_key = uuid.uuid4()
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=test_commit_key,
                source_commit_id=test_commit_source_id,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])
        # This is what the output from a resolved work items commmits operation will look like.
        resolved_work_items_commits = [
            dict(
                work_item_key=new_key,
                work_items_source_key=work_item_source.key,
                commit_key=test_commit_key,
                repository_key=test_repo.key,
            )
        ]

        result = aggregations.infer_projects_repositories_relationships(organization.key, resolved_work_items_commits)
        assert result['success']
        assert result['new_relationships'] == []

        assert db.connection().execute(
            f"select count(*) from analytics.projects_repositories "
            f"where project_id={test_project.id} and repository_id={test_repo.id}"
        ).scalar() == 1