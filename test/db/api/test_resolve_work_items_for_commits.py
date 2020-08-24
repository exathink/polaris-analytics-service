# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.analytics.db import api

from test.fixtures.work_item_commit_resolution import *


class TestRepoScope:

    def it_returns_a_match_when_there_is_a_commit_matching_the_work_item(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
            items_data=new_work_items
        )
        test_commit_source_id = '00001'
        test_commit_key = uuid.uuid4()
        test_commits = [
            dict(
                repository_id=test_repo.id,
                key=test_commit_key.hex,
                source_commit_id=test_commit_source_id,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ]
        create_test_commits(test_commits)

        result = api.resolve_work_items_for_commits(test_organization_key, test_repo.key, test_commits)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['commit_key'] == str(test_commit_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_sets_the_delivery_cycle_of_the_commit_to_the_current_delivery_cycle_of_the_work_item(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
            items_data=new_work_items
        )
        test_commit_source_id = '00001'
        test_commit_key = uuid.uuid4()
        test_commits = [
            dict(
                repository_id=test_repo.id,
                key=test_commit_key.hex,
                source_commit_id=test_commit_source_id,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ]
        create_test_commits(test_commits)

        result = api.resolve_work_items_for_commits(test_organization_key, test_repo.key, test_commits)
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.work_items_commits inner join analytics.work_items "
            f"on work_items_commits.work_item_id = work_items.id and work_items_commits.delivery_cycle_id = work_items.current_delivery_cycle_id"
        ).scalar() == 1

    def it_returns_a_valid_map_when_there_are_multiple_matching_commits_and_work_items(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        key_1000 = uuid.uuid4().hex
        key_1002 = uuid.uuid4().hex
        new_work_items = [
            dict(
                key=key_1000,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            ),
            dict(
                key=key_1002,
                display_id='1002',
                created_at=get_date("2018-12-02"),
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                display_id='1003',
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
            items_data=new_work_items
        )

        commit_1000_key = uuid.uuid4().hex
        commit_1002_key = uuid.uuid4().hex
        test_commits = [
            dict(
                repository_id=test_repo.id,
                key=commit_1000_key,
                source_commit_id=commit_1000_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            ),
            dict(
                repository_id=test_repo.id,
                key=commit_1002_key,
                source_commit_id=commit_1002_key,
                commit_message="Another change. Fixes issue #1002",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            ),
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4().hex,
                source_commit_id=uuid.uuid4().hex,
                commit_message="Another change. Fixes no issues",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ]
        create_test_commits(test_commits)

        result = api.resolve_work_items_for_commits(test_organization_key, test_repo.key, test_commits)
        assert result['success']
        assert len(result['resolved']) == 2

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 2


class TestOrgScope:

    def it_returns_a_valid_match_when_the_commit_mapping_scope_is_organization(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='github',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        test_commits = [
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4().hex,
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ]
        create_test_commits(test_commits)

        result = api.resolve_work_items_for_commits(test_organization_key, test_repo.key, test_commits)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1


class TestProjectScope:

    def it_matches_work_items_attached_at_project_scope(self, commits_fixture):
        organization, projects, repositories, _ = commits_fixture
        mercury = projects['mercury']
        alpha = repositories['alpha']

        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='github',
                commit_mapping_scope='project',
                commit_mapping_scope_key=mercury.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_commit_key = '00001'
        test_commits = [
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4().hex,
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ]
        create_test_commits(test_commits)

        result = api.resolve_work_items_for_commits(test_organization_key, alpha.key, test_commits)
        assert result['success']
        assert len(result['resolved']) == 1
        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_matches_across_work_items_across_multiple_projects_when_the_repo_belongs_to_multiple_projects(
            self, commits_fixture
    ):
        organization, projects, repositories, _ = commits_fixture
        mercury = projects['mercury']
        venus = projects['venus']
        # alpha belongs to both mercury and venus
        alpha = repositories['alpha']

        mercury_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='github',
                commit_mapping_scope='project',
                commit_mapping_scope_key=mercury.key,
                **work_items_source_common
            ),
            items_data=[
                dict(
                    key=uuid.uuid4().hex,
                    display_id='1000',
                    created_at=get_date("2018-12-02"),
                    **work_items_common
                )
            ]
        )

        venus_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='github',
                commit_mapping_scope='project',
                commit_mapping_scope_key=venus.key,
                **work_items_source_common
            ),
            items_data=[
                dict(
                    key=uuid.uuid4().hex,
                    display_id='1001',
                    created_at=get_date("2018-12-02"),
                    **work_items_common
                )
            ]
        )

        test_commits = [
            # This commit references a work item in mercury
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4().hex,
                source_commit_id='00001',
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            ),
            # This commit references a work item in venus
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4().hex,
                source_commit_id='00002',
                commit_message="Another change. Fixes issue #1001",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ]
        create_test_commits(test_commits)

        result = api.resolve_work_items_for_commits(test_organization_key, alpha.key, test_commits)
        assert result['success']
        assert len(result['resolved']) == 2
        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 2
