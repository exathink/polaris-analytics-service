# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.commit_history_imported import *

from polaris.analytics.db import api, model

def init_contributors(contributor_aliases):
    with db.orm_session() as session:
        for ca in contributor_aliases:
            contributor = model.Contributor(
                name=ca['name'],
                key=ca['contributor_key']
            )
            contributor.aliases.append(
                model.ContributorAlias(
                    name=ca['name'],
                    key=ca['contributor_key'],
                    source_alias=ca['source_alias'],
                    source=ca['source']
                )
            )
            session.add(contributor)

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


    def it_maps_contributors_and_aliases_correctly(self, cleanup):

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

        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2

        assert db.connection().execute(f"select count(id) from analytics.commits where committer_contributor_key='{joe_contributor_key}'").scalar() == 1
        assert db.connection().execute(f"select count(id) from analytics.commits where author_contributor_key='{billy_contributor_key}'").scalar() == 1


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

        init_contributors([
            dict(
                name='Joe Blow',
                contributor_key=joe_contributor_key,
                source_alias='joe@blow.com',
                source='vcs'
            ),
            dict(
                name='Billy Bob',
                contributor_key=billy_contributor_key,
                source_alias='billy@bob.com',
                source='vcs'
            )])

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
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2


    def it_imports_a_single_new_commit_with_existing_and_new_contributors(self, cleanup):
        init_contributors([
            dict(
                name='Joe Blow',
                contributor_key=joe_contributor_key,
                source_alias='joe@blow.com',
                source='vcs'
            )
        ])
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
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2

    def it_imports_multiple_commit_with_existing_and_new_contributors(self, cleanup):
        init_contributors([
            dict(
                name='Joe Blow',
                contributor_key=joe_contributor_key,
                source_alias='joe@blow.com',
                source='vcs'
            )
        ])
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
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2

    def it_is_idempotent(self, cleanup):
        init_contributors([
            dict(
                name='Joe Blow',
                contributor_key=joe_contributor_key,
                source_alias='joe@blow.com',
                source='vcs'
            )
        ])
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
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2


@pytest.fixture()
def import_commit_details_fixture(cleanup):
    with db.create_session() as session:
        contributor_id = session.connection.execute(
            model.contributors.insert(
                dict(
                    name='Joe Blow',
                    key=joe_contributor_key
                )
            )
        ).inserted_primary_key[0]

        contributor_alias_id = session.connection.execute(
            model.contributor_aliases.insert(
                dict(
                    name='Joe Blow',
                    key=joe_contributor_key,
                    source='vcs',
                    source_alias='joe@blow.com',
                    contributor_id=contributor_id
                )
            )
        ).inserted_primary_key[0]

        session.connection.execute(
            model.commits.insert([
                dict(
                    repository_key=rails_repository_key,
                    organization_key=rails_organization_key,
                    source_commit_id=f'{key}',
                    key=uuid.uuid4().hex,
                    committer_contributor_alias_id=contributor_alias_id,
                    author_contributor_alias_id=contributor_alias_id,
                    commit_date=datetime.utcnow(),
                    commit_date_tz_offset=0,
                    committer_contributor_key=joe_contributor_key,
                    committer_contributor_name="joe@blow.com",
                    author_date=datetime.utcnow(),
                    author_date_tz_offset=0,
                    author_contributor_key=billy_contributor_key,
                    author_contributor_name="billy",
                    created_at=datetime.utcnow(),
                    commit_message='a change'
                )
                for key in range(1000, 1010)
            ])
        )

class TestImportCommitDetails:

    def it_updates_commit_details_for_a_single_commit(self, import_commit_details_fixture):
        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id='1000',
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    )
                )
            ]
        )
        result = api.import_commit_details(**payload)
        assert result['success']
        assert result['commits_updated'] == 1

        updated = db.connection().execute("select parents, stats, num_parents from analytics.commits where source_commit_id='1000'").first()
        assert updated.parents == ['99', '100']
        assert updated.stats
        assert updated.num_parents == 2

    def it_updates_commit_details_for_multiple_commits(self, import_commit_details_fixture):
        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id=f"{key}",
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    )
                )
                for key in range(1000, 1010)
            ]
        )
        result = api.import_commit_details(**payload)
        assert result['success']
        assert result['commits_updated'] == 10

        updated = db.connection().execute("select parents, stats, num_parents from analytics.commits where source_commit_id='1000'").first()
        assert updated.parents == ['99', '100']
        assert updated.stats
        assert updated.num_parents == 2




