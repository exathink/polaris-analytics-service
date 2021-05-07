# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.analytics.db import api

from polaris.analytics.db.model import work_items as work_items_impl
from test.fixtures.work_item_commit_resolution import *
from polaris.utils.collections import dict_merge


class TestSingleRepo:

    def it_returns_a_match_when_there_is_a_commit_matching_the_work_item(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
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
            items_data=new_work_items
        )
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

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
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
            items_data=new_work_items
        )
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

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.work_items_commits inner join analytics.work_items "
            f"on work_items_commits.work_item_id = work_items.id and work_items_commits.delivery_cycle_id = work_items.current_delivery_cycle_id"
        ).scalar() == 1

    def it_returns_a_match_when_the_commit_is_on_a_branch_matching_the_work_item(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
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
            items_data=new_work_items
        )
        test_commit_source_id = '00001'
        test_commit_key = uuid.uuid4()
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=test_commit_key,
                source_commit_id=test_commit_source_id,
                commit_message="Another change.",
                author_date=get_date("2018-12-03"),
                **dict_merge(
                    commits_common_fields(commits_fixture),
                    dict(created_on_branch="1000")
                )
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['commit_key'] == str(test_commit_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_returns_a_valid_map_when_there_are_multiple_matching_commits_and_work_items(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        key_1000 = uuid.uuid4()
        key_1002 = uuid.uuid4()
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
                key=uuid.uuid4(),
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

        commit_1000_key = uuid.uuid4()
        commit_1002_key = uuid.uuid4()
        create_test_commits([
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
                key=uuid.uuid4(),
                source_commit_id=uuid.uuid4(),
                commit_message="Another change. Fixes no issues",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 2

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 2

    def it_returns_a_valid_match_when_the_commit_mapping_scope_is_organization(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
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
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_only_matches_commits_that_have_author_date_after_create_date_of_work_items(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
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
            items_data=new_work_items
        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-01"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 0

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 0

    def it_only_matches_commits_that_have_commit_date_and_author_date_after_create_date_of_work_items(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
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
            items_data=new_work_items
        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **dict_merge(
                    commits_common_fields(commits_fixture),
                    dict(commit_date=get_date("2018-12-04"))
                )
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_does_not_match_commits_that_have_commit_date_before_create_date_of_work_items(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='999',
                created_at=get_date("2018-12-02"),
                **work_items_common
            ),
            dict(
                key=uuid.uuid4(),
                display_id='1000',
                created_at=get_date("2019-12-02"),
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
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **dict_merge(
                    commits_common_fields(commits_fixture),
                    dict(commit_date=get_date("2019-12-01"))
                )
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 0

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 0


class TestMultipleRepos:

    def it_returns_a_valid_map_when_there_is_a_commit_matching_the_work_item_at_org_scope(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        alpha = repositories['alpha']
        beta = repositories['beta']

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
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            ),
            dict(
                repository_id=beta.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Also changed here to Fixes issue #1000",
                author_date=get_date("2018-12-04"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 2
        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 2

    def it_returns_a_valid_match_when_there_is_a_commit_matching_the_work_item_at_repo_scope(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        # There are two repos with commits
        alpha = repositories['alpha']
        beta = repositories['beta']

        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            )
        ]
        # Work item source is scoped at alpha, so it should only match work items
        # for commits from alpha
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='github',
                commit_mapping_scope='repository',
                commit_mapping_scope_key=alpha.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )

        test_commit_key = '00001'

        # both repos contain a ref to the work_item 1000
        alpha_commit_key = uuid.uuid4()
        create_test_commits([
            dict(
                repository_id=alpha.id,
                key=alpha_commit_key,
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            ),
            dict(
                repository_id=beta.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Also changed here to Fixes issue #1000",
                author_date=get_date("2018-12-04"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']

        # only alpha commit should be matched.
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['commit_key'] == str(alpha_commit_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_returns_a_valid_map_when_there_is_a_commit_matching_the_work_item_at_project_scope(self,
                                                                                              commits_fixture):
        organization, projects, repositories, _ = commits_fixture
        mercury = projects['mercury']
        alpha = repositories['alpha']
        beta = repositories['beta']

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
                commit_mapping_scope='project',
                commit_mapping_scope_key=mercury.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            ),
            dict(
                repository_id=beta.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Also changed here to Fixes issue #1000",
                author_date=get_date("2018-12-04"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 2
        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 2


class TestPaging:
    work_items_impl.map_display_ids_to_commits_page_size = 5

    def it_returns_a_valid_map_when_there_are_more_commits_than_the_page_size(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        key_1000 = uuid.uuid4()
        key_1002 = uuid.uuid4()
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
                key=uuid.uuid4(),
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
        default_page_size = work_items_impl.map_display_ids_to_commits_page_size
        work_items_impl.map_display_ids_to_commits_page_size = 5
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=uuid.uuid4(),
                commit_message=f"Change {i}. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
            for i in range(0, 11)
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        work_items_impl.map_display_ids_to_commits_page_size = default_page_size
        assert result['success']
        assert len(result['resolved']) == 11
        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 11


class TestTrelloWorkItemCommits:

    def it_returns_a_valid_match_when_display_id_matches(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                commit_identifiers=['1000', 'https://trello.com/c/x28QspUQ', 'x28QspUQ'],
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='trello',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_returns_a_valid_match_when_url_matches(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                commit_identifiers=['1000', 'trello.com/c/x28QspUQ', 'x28QspUQ'],
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='trello',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue trello.com/c/x28QspUQ",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_returns_display_id_match_when_commit_identifiers_is_empty_list(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                commit_identifiers=[],
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='trello',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_returns_no_match_when_commit_identifiers_is_empty_list_and_display_id_is_not_matched(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                commit_identifiers=[],
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='trello',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue trello.com/c/x28QspUQ",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 0

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 0

    def it_returns_display_id_match_when_commit_identifiers_is_null(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                commit_identifiers=None,
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='trello',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_returns_no_match_when_commit_identifiers_is_null_and_display_id_is_not_matched(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                commit_identifiers=None,
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='trello',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue trello.com/c/x28QspUQ",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 0

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 0


class TestJiraWorkItemCommits:

    def it_returns_a_valid_match_when_display_id_matches(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='PX-11',
                created_at=get_date("2018-12-02"),
                commit_identifiers=['PX-11', 'Px-11', 'px-11'],
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='jira',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue PX-11",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_returns_a_valid_match_when_display_id_is_capitalized(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='PX-11',
                created_at=get_date("2018-12-02"),
                commit_identifiers=['PX-11', 'Px-11', 'px-11'],
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='jira',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue Px-11",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1

    def it_returns_a_valid_match_when_display_id_id_lowercased(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key,
                display_id='PX-11',
                created_at=get_date("2018-12-02"),
                commit_identifiers=['PX-11', 'Px-11', 'px-11'],
                **work_items_common
            )
        ]
        work_item_source = setup_work_items(
            organization,
            source_data=dict(
                integration_type='jira',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=test_organization_key,
                **work_items_source_common
            ),
            items_data=new_work_items

        )
        test_commit_key = '00001'
        create_test_commits([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_commit_id=test_commit_key,
                commit_message="Another change. Fixes issue px-11",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ])

        result = api.resolve_commits_for_new_work_items(test_organization_key, work_item_source.key, new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_commits").scalar() == 1
