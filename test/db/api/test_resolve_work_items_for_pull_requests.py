# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from polaris.analytics.db import api

from test.fixtures.work_item_pull_request_resolution import *
from polaris.utils.collections import dict_merge


class TestRepoScope:

    def it_returns_a_match_when_pull_request_title_matches_the_work_item(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(display_id='1000')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
        assert result['success']
        assert len(result['resolved']) == 1
        assert result['resolved'][0]['pull_request_key'] == str(test_pr_key)
        assert result['resolved'][0]['work_item_key'] == str(new_key)
        assert result['resolved'][0]['work_items_source_key'] == str(work_item_source.key)
        assert result['resolved'][0]['repository_key'] == str(test_repo.key)

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 1

    def it_returns_a_match_when_pull_request_source_branch_matches_the_work_item(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(source_branch='1000')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(description="Another change. Fixes issue #1000")
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

        pr_1000_key = uuid.uuid4().hex
        pr_1002_key = uuid.uuid4().hex
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=pr_1000_key,
                source_id=pr_1000_key,
                source_repository_id=test_repo.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            ),
            dict(
                repository_id=test_repo.id,
                key=pr_1002_key,
                source_id=pr_1002_key,
                source_repository_id=test_repo.id,
                title="Another change. Fixes issue #1002",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            ),
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4().hex,
                source_id=uuid.uuid4().hex,
                source_repository_id=test_repo.id,
                title="Another change. Fixes no issues",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
        assert result['success']
        assert len(result['resolved']) == 2

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 2


class TestOrgScope:

    def it_returns_a_valid_match_when_the_mapping_scope_is_organization(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
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
        test_pr_key = '00001'
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=uuid.uuid4().hex,
                source_id=test_pr_key,
                source_repository_id=test_repo.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
        assert result['success']
        assert len(result['resolved']) == 1

        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 1


class TestProjectScope:

    def it_matches_work_items_attached_at_project_scope(self, pull_requests_fixture):
        organization, projects, repositories = pull_requests_fixture
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
        test_pr_key = '00001'
        test_pull_requests = [
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4().hex,
                source_id=test_pr_key,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, alpha.key, test_pull_requests)
        assert result['success']
        assert len(result['resolved']) == 1
        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 1

    def it_matches_across_work_items_across_multiple_projects_when_the_repo_belongs_to_multiple_projects(
            self, pull_requests_fixture
    ):
        organization, projects, repositories = pull_requests_fixture
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

        test_pull_requests = [
            # This pull_request references a work item in mercury
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4().hex,
                source_id='00001',
                source_repository_id=alpha.id,
                title="Another change. Fixes issue #1000",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            ),
            # This pull_request references a work item in venus
            dict(
                repository_id=alpha.id,
                key=uuid.uuid4().hex,
                source_id='00002',
                source_repository_id=alpha.id,
                title="Another change. Fixes issue #1001",
                created_at=get_date("2018-12-03"),
                **pull_requests_common_fields()
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, alpha.key, test_pull_requests)
        assert result['success']
        assert len(result['resolved']) == 2
        assert db.connection().execute("select count(*) from analytics.work_items_pull_requests").scalar() == 2


class TestTrelloPullRequestMapping:

    def it_returns_a_valid_match_when_pull_request_display_id_matches_display_id(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(display_id='1000')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

    def it_returns_a_valid_match_when_pull_request_display_id_matches_url(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(display_id='trello.com/c/x28QspUQ')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

    def it_returns_a_valid_match_when_pull_request_title_matches_display_id(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change. closes #1000",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(display_id='trello.com/1000')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

    def it_returns_a_valid_match_when_pull_request_title_matches_url(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change, Resolves trello.com/c/x28QspUQ",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(display_id='trello.com')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

    def it_returns_a_valid_match_when_pull_request_branch_matches_display_id(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(source_branch='1000')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

    def it_returns_a_valid_match_when_pull_request_branch_matches_url(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(source_branch='trello.com/c/x28QspUQ')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

    def it_returns_a_valid_match_when_pull_request_description_matches_display_id(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(description='Resolves card #1000')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

    def it_returns_a_valid_match_when_pull_request_description_matches_url(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(description='Resolves trello.com/c/x28QspUQ')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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

    def it_returns_a_valid_match_when_there_are_multiple_matches(self, pull_requests_fixture):
        organization, _, repositories = pull_requests_fixture
        test_repo = repositories['alpha']
        new_key = uuid.uuid4()
        new_work_items = [
            dict(
                key=new_key.hex,
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
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=new_work_items
        )
        test_pr_source_id = '00001'
        test_pr_key = uuid.uuid4()
        test_pull_requests = [
            dict(
                repository_id=test_repo.id,
                key=test_pr_key.hex,
                source_id=test_pr_source_id,
                source_repository_id=test_repo.id,
                title="Another change. Fixes card #1000",
                created_at=get_date("2018-12-03"),
                **dict_merge(
                    pull_requests_common_fields(),
                    dict(display_id='trello.com/c/x28QspUQ')
                )
            )
        ]
        create_test_pull_requests(test_pull_requests)

        result = api.resolve_work_items_for_pull_requests(test_organization_key, test_repo.key, test_pull_requests)
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
