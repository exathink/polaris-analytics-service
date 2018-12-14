# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.commit_history_imported import *

from polaris.analytics.db import api, model

class TestCommitImport:

    def it_imports_a_single_new_commit_with_new_committer_and_author(self, cleanup):

        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= [
                dict(
                    source_commit_id='XXXX',
                    **commit_common_fields
                )
            ],
            new_contributors= [
                dict(
                    name='Joe Blow',
                    contributor_key=joe_contributor_key,
                    alias='joe@blow.com'
                ),
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert len(result['new_commits']) == 1
        assert len(result['new_contributors']) == 2

        # it assigns keys to new commits
        assert all(map(lambda commit: commit.get('key'), result['new_commits']))
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 1
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2

    def it_returns_a_valid_result_object(self, cleanup):
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    **commit_common_fields
                )
            ],
            new_contributors=[
                dict(
                    name='Joe Blow',
                    contributor_key=joe_contributor_key,
                    alias='joe@blow.com'
                ),
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert len(result['new_commits']) == 1
        assert len(result['new_contributors']) == 2
        assert all(map(lambda commit: commit.get('key'), result['new_commits']))


    def it_imports_a_single_new_commit_with_existing_contributors(self, cleanup):

        db.connection().execute(
            model.contributors.insert(
                [
                    dict(
                        name='Joe Blow',
                        key=joe_contributor_key,
                        source_alias='joe@blow.com',
                        source='vcs'
                    ),
                    dict(
                        name='Billy Bob',
                        key=billy_contributor_key,
                        source_alias='billy@bob.com',
                        source='vcs'
                    )
                ]
            )
        )
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= [
                dict(
                    source_commit_id='XXXX',
                    **commit_common_fields
                )
            ],
            new_contributors= []
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 1
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2

    def it_imports_a_single_new_commit_with_existing_and_new_contributors(self, cleanup):

        db.connection().execute(
            model.contributors.insert(
                [
                    dict(
                        name='Joe Blow',
                        key=joe_contributor_key,
                        source_alias='joe@blow.com',
                        source='vcs'
                    )
                ]
            )
        )
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= [
                dict(
                    source_commit_id='XXXX',
                    **commit_common_fields
                )
            ],
            new_contributors= [
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 1
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2

    def it_imports_multiple_commit_with_existing_and_new_contributors(self, cleanup):

        db.connection().execute(
            model.contributors.insert(
                [
                    dict(
                        name='Joe Blow',
                        key=joe_contributor_key,
                        source_alias='joe@blow.com',
                        source='vcs'
                    )
                ]
            )
        )
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= [
                dict(
                    source_commit_id=f'XXXX-{i}',
                    **commit_common_fields
                )
                for i in range(0,9)
            ],
            new_contributors= [
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 9
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2

    def it_is_idempotent(self, cleanup):

        db.connection().execute(
            model.contributors.insert(
                [
                    dict(
                        name='Joe Blow',
                        key=joe_contributor_key,
                        source_alias='joe@blow.com',
                        source='vcs'
                    )
                ]
            )
        )
        args=dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id=f'XXXX-{i}',
                    **commit_common_fields
                )
                for i in range(0, 9)
            ],
            new_contributors=[
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )
        # first import once
        api.import_new_commits(**args)
        # now import again
        result = api.import_new_commits(**args)

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 9
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2