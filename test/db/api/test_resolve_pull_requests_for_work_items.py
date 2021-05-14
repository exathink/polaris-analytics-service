# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from polaris.analytics.db import api

from polaris.analytics.db.model import work_items as work_items_impl
from test.fixtures.work_item_pull_request_resolution import *
from polaris.utils.collections import dict_merge


class TestSingleRepo:

    def it_returns_a_match_when_pull_request_title_matches_the_work_item(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        # Check for delivery cycle id too
        assert db.connection().execute(f"select count(*) from analytics.work_items_pull_requests "
                                       f"join analytics.work_items on work_items.id=work_items_pull_requests.work_item_id "
                                       f"where delivery_cycle_id=current_delivery_cycle_id").scalar() == 1

    def it_returns_a_match_when_pull_request_display_id_matches_the_work_item(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change.",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(source_branch='1000')
                )
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 1

    def it_returns_a_match_when_pull_request_description_matches_the_work_item(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Resolved",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(description="Another change. Fixes issue #1000")
                )
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 1

    def it_returns_a_match_when_the_pull_request_source_branch_matches_the_work_item(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change. Fixes issue",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(source_branch='1000')
                )
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 1

    def it_returns_a_valid_map_when_there_are_multiple_matching_pull_requests_and_work_items(self,
                                                                                             pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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

        pr_1000_key = uuid.uuid4()
        pr_1002_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=pr_1000_key,
                source_id=pr_1000_key,
                source_repository_id=test_repo.id,
                title="Another change. Fixes issues #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            ),
            dict(
                repository_id=test_repo.id,
                key=pr_1002_key,
                source_id=pr_1002_key,
                source_repository_id=test_repo.id,
                title="Another change. Fixes no issues",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            ),
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_id=uuid.uuid4(),
                source_repository_id=test_repo.id,
                title="Another change. Fixes issue #1002",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 2

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 2

    def it_returns_a_valid_match_when_the_mapping_scope_is_organization(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_key = '00001'
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_id=test_pr_key,
                source_repository_id=test_repo.id,
                title="Resolved issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 1

    def it_only_matches_pull_requests_that_have_create_date_after_create_date_of_work_items(self,
                                                                                            pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_key = '00001'
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_id=test_pr_key,
                source_repository_id=test_repo.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-01"),
                **pull_requests_common_fields()
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 0

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 0


class TestMultipleRepos:

    def it_returns_a_valid_map_when_there_is_a_pull_request_matching_the_work_item_at_org_scope(self,
                                                                                                pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_key = '00001'
        create_test_pull_requests([
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4(),
                source_id=test_pr_key,
                source_repository_id=alpha.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            ),
            dict(
                repository_id=beta.id,
                key=uuid.uuid4(),
                source_id=test_pr_key,
                source_repository_id=beta.id,
                title="Also changed here to Fixes issue #1000",
                created_at=get_date("2018-12-04"),
                **pull_requests_common_fields()
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 2
        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 2

    def it_returns_a_valid_match_when_there_is_a_pull_request_matching_the_work_item_at_repo_scope(self,
                                                                                                   pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        # There are two repos with pull requests
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
        # for pull requests to alpha
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

        test_pr_key = '00001'

        # both repos contain a ref to the work_item 1000
        alpha_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=alpha.id,
                key=alpha_pr_key,
                source_id=test_pr_key,
                source_repository_id=alpha.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            ),
            dict(
                repository_id=beta.id,
                key=uuid.uuid4(),
                source_id=test_pr_key,
                source_repository_id=alpha.id,
                title="Also changed here to Fixes issue #1000",
                created_at=get_date("2018-12-04"),
                **pull_requests_common_fields()
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']

        # only alpha pull requests should be matched.
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(alpha_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 1

    def it_returns_a_valid_map_when_there_is_a_pull_request_matching_the_work_item_at_project_scope(self,
                                                                                                    pull_requests_fixture):
        organization, projects, repositories = pull_requests_fixture
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
        test_pr_key = '00001'
        create_test_pull_requests([
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4(),
                source_id=test_pr_key,
                source_repository_id=alpha.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            ),
            dict(
                repository_id=beta.id,
                key=uuid.uuid4(),
                source_id=test_pr_key,
                source_repository_id=alpha.id,
                title="Also changed here to Fixes issue #1000",
                created_at=get_date("2018-12-04"),
                **pull_requests_common_fields()
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 2
        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 2


class TestPaging:
    work_items_impl.map_commit_identifiers_to_pull_requests_page_size = 5

    def it_returns_a_valid_map_when_there_are_more_pull_requests_than_the_page_size(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        default_page_size = work_items_impl.map_commit_identifiers_to_pull_requests_page_size
        work_items_impl.map_commit_identifiers_to_pull_requests_page_size = 5
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4(),
                source_id=uuid.uuid4(),
                source_repository_id=test_repo.id,
                title=f"Change {i}. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
            for i in range(0, 11)
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        work_items_impl.map_commit_identifiers_to_pull_requests_page_size = default_page_size
        assert result['success']
        assert len(result['resolved']) == 11
        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 11


class TestTrelloWorkItemPullRequests:

    def it_returns_a_match_when_pull_request_title_matches_the_work_item_display_id(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change. Fixes card #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        # Check for delivery cycle id too
        assert db.connection().execute(f"select count(*) from analytics.work_items_pull_requests "
                                       f"join analytics.work_items on work_items.id=work_items_pull_requests.work_item_id ").scalar() == 1

    def it_returns_a_match_when_pull_request_description_matches_the_work_item_display_id(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(description='Another change. Closes #1000')
                )
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        # Check for delivery cycle id too
        assert db.connection().execute(f"select count(*) from analytics.work_items_pull_requests "
                                       f"join analytics.work_items on work_items.id=work_items_pull_requests.work_item_id ").scalar() == 1

    def it_returns_a_match_when_pull_request_branch_matches_the_work_item_display_id(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(source_branch='1000')
                )
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        # Check for delivery cycle id too
        assert db.connection().execute(f"select count(*) from analytics.work_items_pull_requests "
                                       f"join analytics.work_items on work_items.id=work_items_pull_requests.work_item_id ").scalar() == 1

    def it_returns_a_match_when_pull_request_title_matches_the_work_item_url(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change. Closes trello.com/c/x28QspUQ",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(display_id='1010')
                )
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        # Check for delivery cycle id too
        assert db.connection().execute(f"select count(*) from analytics.work_items_pull_requests "
                                       f"join analytics.work_items on work_items.id=work_items_pull_requests.work_item_id ").scalar() == 1

    def it_returns_a_match_when_pull_request_description_matches_the_work_item_url(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        create_test_pull_requests([
            dict(
                repository_id=test_repo.id,
                key=test_pr_key,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(description='Another change. Closes trello.com/c/x28QspUQ')
                )
            )
        ])

        result = api.resolve_pull_requests_for_new_work_items(test_organization_key, work_item_source.key,
                                                              new_work_items)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        # Check for delivery cycle id too
        assert db.connection().execute(f"select count(*) from analytics.work_items_pull_requests "
                                       f"join analytics.work_items on work_items.id=work_items_pull_requests.work_item_id ").scalar() == 1
