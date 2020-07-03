# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


import uuid
from test.fixtures.repo_org import *
from test.fixtures.pull_requests import pull_requests_common
from polaris.analytics.db import api
from polaris.common import db


class TestCreatePullRequests:

    def it_imports_new_pull_requests(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]
        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 10

    def it_only_creates_new_pull_requests(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]

        api.import_new_pull_requests(rails_repository_key, pr_summaries)

        pr_summaries.append(
            dict(
                key=uuid.uuid4().hex,
                source_id=100,
                display_id='new',
                **pull_requests_common()
            )
        )

        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        assert result['insert_count'] == 1
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 11

    def it_is_idempotent(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]
        api.import_new_pull_requests(rails_repository_key, pr_summaries)
        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        assert result['insert_count'] == 0
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 10

    def it_updates_source_repository_id_for_created_pull_requests(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]
        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        test_pr_key = pr_summaries[0]['key']
        assert db.connection().execute(f"select source_repository_id from analytics.pull_requests where key='{test_pr_key}'").scalar() == repository_id

    def it_throws_exception_when_repository_does_not_exist(self, setup_org):
        organization = setup_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]
        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert not result['success']
        assert result['exception'] == f"Could not find repository with key: {rails_repository_key}"
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 0


class TestUpdatePullRequests:

    def it_updates_state(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]

        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 10

        test_pr_key = pr_summaries[0]['key']
        pr_summaries[0]['state'] = 'merged'
        result = api.update_pull_requests(rails_repository_key, [pr_summaries[0]])
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.pull_requests where key='{test_pr_key}' and state='merged'").scalar() == 1

    def it_updates_updated_at(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]

        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 10

        test_pr_key = pr_summaries[0]['key']
        pr_summaries[0]['updated_at'] = "2020-06-24 01:53:48.171000"
        result = api.update_pull_requests(rails_repository_key, [pr_summaries[0]])
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.pull_requests where key='{test_pr_key}' and updated_at='2020-06-24 01:53:48.171000'").scalar() == 1

    def it_updates_merge_status(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]

        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 10

        test_pr_key = pr_summaries[0]['key']
        pr_summaries[0]['merge_status'] = "ready to merge"
        result = api.update_pull_requests(rails_repository_key, [pr_summaries[0]])
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.pull_requests where key='{test_pr_key}' and merge_status='ready to merge'").scalar() == 1

    def it_updates_title(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]

        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 10

        test_pr_key = pr_summaries[0]['key']
        pr_summaries[0]['title'] = "WIP: PR"
        result = api.update_pull_requests(rails_repository_key, [pr_summaries[0]])
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.pull_requests where key='{test_pr_key}' and title='WIP: PR'").scalar() == 1

    def it_updates_source_branch_latest_commit(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org
        pr_summaries = [
            dict(
                key=uuid.uuid4().hex,
                source_id=str(i),
                display_id=str(i),
                **pull_requests_common()
            )
            for i in range(0, 10)
        ]

        result = api.import_new_pull_requests(rails_repository_key, pr_summaries)
        assert result['success']
        assert db.connection().execute('select count(id) from analytics.pull_requests').scalar() == 10

        test_pr_key = pr_summaries[0]['key']
        pr_summaries[0]['source_branch_latest_commit'] = "NewCommit"
        result = api.update_pull_requests(rails_repository_key, [pr_summaries[0]])
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.pull_requests where key='{test_pr_key}' and source_branch_latest_commit='NewCommit'").scalar() == 1
