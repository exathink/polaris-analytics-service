# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.analytics.db import commands
from test.fixtures.work_item_commit_resolution import *


class TestUpdateCommitWorkItemSummaries:

    def it_updates_correctly_for_a_single_work_item_and_commit(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        work_item_key = uuid.uuid4()
        work_items = [
            dict(
                key=work_item_key.hex,
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
            items_data=work_items
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

        work_items_commits = [
            dict(
                commit_key=test_commit_key,
                work_item_key=work_item_key
            )
        ]
        resolved = commands.update_commit_work_item_summaries(organization.key, work_items_commits)
        assert resolved['success']
        saved = db.connection().execute(
            f"select work_items_summaries from analytics.commits where key='{test_commit_key}'").first()
        assert saved.work_items_summaries == [
            dict(
                key=work_item_key.hex,
                name=work_items_common['name'],
                display_id='1000',
                url=work_items_common['url'],
                work_item_type='issue'
            )
        ]

    def it_updates_correctly_when_a_commit_refers_to_multiple_work_items(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        work_item_key1 = uuid.uuid4()
        work_item_key2 = uuid.uuid4()
        work_items = [
            dict(
                key=work_item_key1.hex,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            ),
            dict(
                key=work_item_key2.hex,
                display_id='1001',
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
            items_data=work_items
        )
        test_commit_source_id = '00001'
        test_commit_key = uuid.uuid4()
        test_commits = [
            dict(
                repository_id=test_repo.id,
                key=test_commit_key.hex,
                source_commit_id=test_commit_source_id,
                commit_message="Another change. Fixes issues #1000, #1002",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )
        ]
        create_test_commits(test_commits)

        work_items_commits = [
            dict(
                commit_key=test_commit_key,
                work_item_key=work_item_key1
            ),
            dict(
                commit_key=test_commit_key,
                work_item_key=work_item_key2
            )
        ]
        resolved = commands.update_commit_work_item_summaries(organization.key, work_items_commits)
        assert resolved['success']
        saved = db.connection().execute(
            f"select work_items_summaries from analytics.commits where key='{test_commit_key}'").first()
        assert saved.work_items_summaries == [
            dict(
                key=work_item_key1.hex,
                name=work_items_common['name'],
                display_id='1000',
                url=work_items_common['url'],
                work_item_type='issue'
            ),
            dict(
                key=work_item_key2.hex,
                name=work_items_common['name'],
                display_id='1001',
                url=work_items_common['url'],
                work_item_type='issue'
            )
        ]

    def it_updates_correctly_when_there_are_multiple_commits(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        work_item_key = uuid.uuid4()
        work_items = [
            dict(
                key=work_item_key.hex,
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
            items_data=work_items
        )

        test_commit_key1 = uuid.uuid4()
        test_commit_key2 = uuid.uuid4()

        test_commits = [
            dict(
                repository_id=test_repo.id,
                key=test_commit_key1.hex,
                source_commit_id='00001',
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            ),
            dict(
                repository_id=test_repo.id,
                key=test_commit_key2.hex,
                source_commit_id='000002',
                commit_message="Another change. Fixes issue #1000",
                author_date=get_date("2018-12-03"),
                **commits_common_fields(commits_fixture)
            )

        ]
        create_test_commits(test_commits)

        work_items_commits = [
            dict(
                commit_key=test_commit_key1,
                work_item_key=work_item_key
            ),
            dict(
                commit_key=test_commit_key2,
                work_item_key=work_item_key
            )
        ]
        resolved = commands.update_commit_work_item_summaries(organization.key, work_items_commits)
        assert resolved['success']
        saved = db.connection().execute(
            f"select work_items_summaries from analytics.commits where key in ('{test_commit_key1}', '{test_commit_key2}')").fetchall()
        assert saved[0].work_items_summaries == [
            dict(
                key=work_item_key.hex,
                name=work_items_common['name'],
                display_id='1000',
                url=work_items_common['url'],
                work_item_type='issue'
            )
        ]
        assert saved[1].work_items_summaries == [
            dict(
                key=work_item_key.hex,
                name=work_items_common['name'],
                display_id='1000',
                url=work_items_common['url'],
                work_item_type='issue'
            )
        ]

    def it_is_idempotent(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        test_repo = repositories['alpha']
        work_item_key = uuid.uuid4()
        work_items = [
            dict(
                key=work_item_key.hex,
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
            items_data=work_items
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

        work_items_commits = [
            dict(
                commit_key=test_commit_key,
                work_item_key=work_item_key
            )
        ]
        # update once
        commands.update_commit_work_item_summaries(organization.key, work_items_commits)
        # update again
        resolved = commands.update_commit_work_item_summaries(organization.key, work_items_commits)
        assert resolved['success']
        saved = db.connection().execute(
            f"select work_items_summaries from analytics.commits where key='{test_commit_key}'").first()
        assert saved.work_items_summaries == [
            dict(
                key=work_item_key.hex,
                name=work_items_common['name'],
                display_id='1000',
                url=work_items_common['url'],
                work_item_type='issue'
            )
        ]
