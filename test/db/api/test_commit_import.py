# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from test.fixtures.commit_history import *
from test.fixtures.commit_details import *

from datetime import timedelta
from polaris.analytics.db import api, model
from polaris.utils.collections import find, Fixture


def init_contributors(contributor_aliases):
    contributors = []
    with db.orm_session() as session:
        for ca in contributor_aliases:
            contributor = model.Contributor(
                name=ca['name'],
                key=ca['key']
            )
            contributor.aliases.append(
                model.ContributorAlias(
                    name=ca['name'],
                    key=ca['key'],
                    source_alias=ca['source_alias'],
                    source=ca['source']
                )
            )
            session.add(contributor)
            contributors.append(contributor)

    return contributors

class TestCommitImport:

    def it_imports_a_single_new_commit_with_new_committer_and_author(self, setup_repo_org):
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
            ],
            new_contributors=[
                dict(
                    name='Joe Blow',
                    key=joe_contributor_key,
                    alias='joe@blow.com'
                ),
                dict(
                    name='Billy Bob',
                    key=billy_contributor_key,
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

    def it_maps_contributors_and_aliases_correctly(self, setup_repo_org):
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
            ],
            new_contributors=[
                dict(
                    name='Joe Blow',
                    key=joe_contributor_key,
                    alias='joe@blow.com'
                ),
                dict(
                    name='Billy Bob',
                    key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert len(result['new_commits']) == 1
        assert len(result['new_contributors']) == 2

        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2

        assert db.connection().execute(
            f"select count(id) from analytics.commits where committer_contributor_key='{joe_contributor_key}'").scalar() == 1
        assert db.connection().execute(
            f"select count(id) from analytics.commits where author_contributor_key='{billy_contributor_key}'").scalar() == 1

    def it_returns_a_valid_result_object(self, setup_repo_org):
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
            ],
            new_contributors=[
                dict(
                    name='Joe Blow',
                    key=joe_contributor_key,
                    alias='joe@blow.com'
                ),
                dict(
                    name='Billy Bob',
                    key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert len(result['new_commits']) == 1
        assert len(result['new_contributors']) == 2
        assert all(map(lambda commit: commit.get('key'), result['new_commits']))

    def it_imports_a_single_new_commit_with_existing_contributors(self, setup_repo_org):
        init_contributors([
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
            )])

        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
            ],
            new_contributors=[]
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 1
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2

    def it_imports_a_single_new_commit_with_existing_and_new_contributors(self, setup_repo_org):
        init_contributors([
            dict(
                name='Joe Blow',
                key=joe_contributor_key,
                source_alias='joe@blow.com',
                source='vcs'
            )
        ])
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
            ],
            new_contributors=[
                dict(
                    name='Billy Bob',
                    key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 1
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2

    def it_imports_multiple_commit_with_existing_and_new_contributors(self, setup_repo_org):
        init_contributors([
            dict(
                name='Joe Blow',
                key=joe_contributor_key,
                source_alias='joe@blow.com',
                source='vcs'
            )
        ])
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id=f'XXXX-{i}',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
                for i in range(0, 9)
            ],
            new_contributors=[
                dict(
                    name='Billy Bob',
                    key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 9
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2
        assert db.connection().execute("select count(id) from analytics.contributor_aliases").scalar() == 2

    def it_is_idempotent(self, setup_repo_org):
        init_contributors([
            dict(
                name='Joe Blow',
                key=joe_contributor_key,
                source_alias='joe@blow.com',
                source='vcs'
            )
        ])
        args = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id=f'XXXX-{i}',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
                for i in range(0, 9)
            ],
            new_contributors=[
                dict(
                    name='Billy Bob',
                    key=billy_contributor_key,
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


class TestCommitTeamAssignment:

    @pytest.yield_fixture()
    def setup(self, setup_repo_org):
        contributors = init_contributors([
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
            )])

        with db.orm_session() as session:
            session.add_all(contributors)
            team_key = uuid.uuid4()
            team = model.Team(
                key=team_key,
                name="Team Alpha"
            )
            organization = model.Organization.find_by_organization_key(session, rails_organization_key)
            organization.teams.append(team)
            for contributor in contributors:
                contributor.assign_to_team(session, team.key)

        yield Fixture(
            team_key=team_key
        )

    def it_resolves_the_team_for_existing_contributors(self, setup):

        fixture = setup

        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
            ],
            new_contributors=[]
        )

        assert result['success']
        row = db.connection().execute(
            "select committer_team_key, committer_team_id, author_team_key, author_team_id from analytics.commits where source_commit_id='XXXX'").fetchone()
        assert row.committer_team_key == fixture.team_key
        assert row.committer_team_id is not None
        assert row.author_team_key == fixture.team_key
        assert row.author_team_id is not None

    def it_updates_teams_repositories(self, setup):
        fixture = setup

        test_commit = dict(
            source_commit_id='XXXX',
            key=uuid.uuid4().hex,
            **commit_common_fields
        )
        test_date = datetime.utcnow()

        test_commit['commit_date'] = test_date
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                test_commit
            ],
            new_contributors=[]
        )

        assert result['success']
        result = db.connection().execute(
            "select repository_id, team_id, commit_count, earliest_commit, latest_commit from analytics.teams_repositories").fetchall()
        assert len(result) == 1
        assert result[0].commit_count == 1
        assert result[0].earliest_commit == test_date
        assert result[0].latest_commit == test_date

    def it_updates_teams_repositories_stats_when_there_are_multiple_commits(self, setup):
        fixture = setup

        test_commits = [
            dict(
                source_commit_id='XXXX',
                key=uuid.uuid4().hex,
                **commit_common_fields
            ),
            dict(
                source_commit_id='YYYY',
                key=uuid.uuid4().hex,
                **commit_common_fields
            ),
            dict(
                source_commit_id='ZZZZ',
                key=uuid.uuid4().hex,
                **commit_common_fields
            )
        ]
        test_date = datetime.utcnow()

        test_commits[0]['commit_date'] = test_date
        test_commits[1]['commit_date'] = test_date - timedelta(days=1)
        test_commits[2]['commit_date'] = test_date + timedelta(days=1)

        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= test_commits,
            new_contributors=[]
        )

        assert result['success']
        result = db.connection().execute(
            "select repository_id, team_id, commit_count, earliest_commit, latest_commit from analytics.teams_repositories").fetchall()
        assert len(result) == 1
        assert result[0].commit_count == 3
        assert result[0].earliest_commit == test_commits[1]['commit_date']

        assert result[0].latest_commit == test_commits[2]['commit_date']


    def it_updates_teams_repositories_stats_correctly_across_batches(self, setup):
        fixture = setup

        test_commits = [
            dict(
                source_commit_id='XXXX',
                key=uuid.uuid4().hex,
                **commit_common_fields
            ),
            dict(
                source_commit_id='YYYY',
                key=uuid.uuid4().hex,
                **commit_common_fields
            ),
            dict(
                source_commit_id='ZZZZ',
                key=uuid.uuid4().hex,
                **commit_common_fields
            )
        ]
        test_date = datetime.utcnow()

        test_commits[0]['commit_date'] = test_date
        test_commits[1]['commit_date'] = test_date - timedelta(days=1)
        test_commits[2]['commit_date'] = test_date + timedelta(days=1)

        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= test_commits[0:2],
            new_contributors=[]
        )

        assert result['success']
        result = db.connection().execute(
            "select repository_id, team_id, commit_count, earliest_commit, latest_commit from analytics.teams_repositories").fetchall()
        assert len(result) == 1
        assert result[0].commit_count == 2
        assert result[0].earliest_commit == test_commits[1]['commit_date']

        assert result[0].latest_commit == test_commits[0]['commit_date']

        # now repeat with the last commit in a second batch
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=test_commits[2:],
            new_contributors=[]
        )

        assert result['success']
        result = db.connection().execute(
            "select repository_id, team_id, commit_count, earliest_commit, latest_commit from analytics.teams_repositories").fetchall()
        assert len(result) == 1
        assert result[0].commit_count == 3
        assert result[0].earliest_commit == test_commits[1]['commit_date']

        assert result[0].latest_commit == test_commits[2]['commit_date']

    def it_does_not_resolve_teams_with_new_committer_and_author(self, setup):
        new_contributor_key = uuid.uuid4().hex
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='YYYY',
                    key=uuid.uuid4().hex,
                    commit_date=datetime.utcnow(),
                    commit_date_tz_offset=0,
                    committer_alias_key=new_contributor_key,
                    author_date=datetime.utcnow(),
                    author_date_tz_offset=0,
                    author_alias_key=new_contributor_key,
                    created_at=datetime.utcnow(),
                    commit_message='a change'
                )
            ],
            new_contributors=[
                dict(
                    name='Joe Blow2',
                    key=new_contributor_key,
                    alias='jo3e@blow.com',
                )
            ]
        )

        assert result['success']
        row = db.connection().execute(
            "select committer_team_key, committer_team_id, author_team_key, author_team_id from analytics.commits where source_commit_id='YYYY'").fetchone()
        assert row.committer_team_key is None
        assert row.committer_team_id is None
        assert row.author_team_key is None
        assert row.author_team_id is None


class TestImportCommitDetails:

    def it_updates_commit_details_for_a_single_commit(self, import_commit_details_fixture):
        keys = import_commit_details_fixture
        source_files = [
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files1.txt',
                file_type='txt',
                version_count=1,
                is_deleted=False,
                action='A',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            ),
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files2.txt',
                file_type='txt',
                version_count=1,
                is_deleted=False,
                action='A',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            ),
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files2.py',
                file_type='py',
                version_count=1,
                is_deleted=False,
                action='U',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            )
        ]

        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id='1000',
                    key=keys[0],
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=source_files
                )
            ]
        )
        result = api.import_commit_details(**payload)
        assert result['success']
        assert result['update_count'] == 1

        updated = db.connection().execute(
            "select parents, stats, num_parents, source_files, source_file_actions_summary, source_file_types_summary from analytics.commits where source_commit_id='1000'").first()
        assert updated.parents == ['99', '100']
        assert updated.stats
        assert updated.num_parents == 2
        assert updated.source_files == source_files

    def it_computes_source_file_type_summary_for_the_commit(self, import_commit_details_fixture):
        keys = import_commit_details_fixture
        source_files = [
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files1.txt',
                file_type='txt',
                version_count=1,
                is_deleted=False,
                action='A',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            ),
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files2.txt',
                file_type='txt',
                version_count=1,
                is_deleted=False,
                action='A',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            ),
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files2.py',
                file_type='py',
                version_count=1,
                is_deleted=False,
                action='U',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            )
        ]

        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id='1000',
                    key=keys[0],
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=source_files
                )
            ]
        )
        result = api.import_commit_details(**payload)
        assert result['success']

        updated = db.connection().execute(
            "select source_file_types_summary from analytics.commits where source_commit_id='1000'").first()

        # we expect an array of the form [{file_type:<name>, count:<count>}, ...]  but we dont the order so we test each
        # expected type separately.
        assert find(updated.source_file_types_summary, lambda s: s['file_type'] == 'py')['count'] == 1
        assert find(updated.source_file_types_summary, lambda s: s['file_type'] == 'txt')['count'] == 2

    def it_computes_source_file_action_summary_for_the_commit(self, import_commit_details_fixture):
        keys = import_commit_details_fixture
        source_files = [
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files1.txt',
                file_type='txt',
                version_count=1,
                is_deleted=False,
                action='A',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            ),
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files2.txt',
                file_type='txt',
                version_count=1,
                is_deleted=False,
                action='A',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            ),
            dict(
                key=uuid.uuid4().hex,
                path='test/',
                name='files2.py',
                file_type='py',
                version_count=1,
                is_deleted=False,
                action='U',
                stats={"lines": 2, "insertions": 2, "deletions": 0}
            )
        ]

        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id='1000',
                    key=keys[0],
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=source_files
                )
            ]
        )
        result = api.import_commit_details(**payload)
        assert result['success']

        updated = db.connection().execute(
            "select source_file_actions_summary from analytics.commits where source_commit_id='1000'").first()

        # we expect an array of the form [{action:<name>, count:<count>}, ...]  but we dont the order so we test each
        # expected type separately.
        assert find(updated.source_file_actions_summary, lambda s: s['action'] == 'A')['count'] == 2
        assert find(updated.source_file_actions_summary, lambda s: s['action'] == 'U')['count'] == 1



    def it_updates_commit_details_for_multiple_commits(self, import_commit_details_fixture):
        keys = import_commit_details_fixture
        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id=f"{key}",
                    key=keys[1000 - key].hex,
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=[
                        dict(
                            key=uuid.uuid4().hex,
                            path='test/',
                            name='files1.txt',
                            file_type='txt',
                            version_count=1,
                            is_deleted=False,
                            action='A',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        ),
                        dict(
                            key=uuid.uuid4().hex,
                            path='test/',
                            name='files2.py',
                            file_type='py',
                            version_count=1,
                            is_deleted=False,
                            action='A',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        )
                    ]
                )
                for key in range(1000, 1010)
            ]
        )
        result = api.import_commit_details(**payload)
        assert result['success']
        assert result['update_count'] == 10

        updated = db.connection().execute(
            "select parents, stats, num_parents from analytics.commits where source_commit_id='1000'").first()
        assert updated.parents == ['99', '100']
        assert updated.stats
        assert updated.num_parents == 2


