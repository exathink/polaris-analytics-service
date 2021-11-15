# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from uuid import UUID
from datetime import datetime
from polaris.analytics.db import commands
from test.fixtures.work_item_commit_resolution import *
from test.fixtures.teams import *

from polaris.analytics.db.model import Contributor, ContributorAlias, Commit
from polaris.common import db
from polaris.utils.collections import Fixture


class TestResolveTeamsForWorkItems:

    def setup_contributors(self, organization, names):
        result = dict()
        with db.orm_session() as session:
            session.expire_on_commit = False
            session.add(organization)
            for name in names:
                contributor = Contributor(
                    name=name,
                    key=uuid.uuid4().hex,

                )
                contributor.aliases.append(
                    ContributorAlias(
                        name=name,
                        key=uuid.uuid4().hex,
                        source='vcs',
                        source_alias=contributor.name
                    )
                )
                organization.contributors.append(contributor)
                result[name] = contributor

        return Fixture(**result)

    def create_test_commits(self, test_commits):
        with db.orm_session() as session:
            session.add_all(
                [Commit(**commit) for commit in test_commits]
            )

    @staticmethod
    def commits_common_fields(test_repo):
        return dict(
            repository_id=test_repo.id,
            commit_message="Another change. Fixes issue #1000",
            commit_date_tz_offset=0,
            author_date_tz_offset=0,
            created_on_branch='master'
        )

    @pytest.fixture()
    def setup(self, org_repo_fixture, setup_teams):
        organization, _, repositories = org_repo_fixture

        contributors = self.setup_contributors(
            organization, names=['joe', 'billy']
        )

        test_repo = repositories['alpha']

        test_work_items = [
            dict(
                key=uuid.uuid4().hex,
                display_id='1000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                display_id='2000',
                created_at=get_date("2018-12-02"),
                **work_items_common
            )
        ]
        setup_work_items(
            organization,
            source_data=dict(
                integration_type='github',
                commit_mapping_scope='repository',
                commit_mapping_scope_key=test_repo.key,
                **work_items_source_common
            ),
            items_data=test_work_items
        )
        team_a = setup_teams.organization.teams[0]
        team_b = setup_teams.organization.teams[1]
        test_commits = [
            dict(

                source_commit_id='XXXX',
                key=uuid.uuid4().hex,
                commit_date=datetime.utcnow(),
                committer_contributor_alias_id=contributors.joe.aliases[0].id,
                committer_contributor_key=contributors.joe.key,
                committer_contributor_name=contributors.joe.name,

                author_contributor_alias_id=contributors.billy.aliases[0].id,
                author_contributor_key=contributors.billy.key,
                author_contributor_name=contributors.billy.name,

                **self.commits_common_fields(test_repo)
            ),
            # author_team is assigned
            dict(

                source_commit_id='YYYY',
                key=uuid.uuid4().hex,
                commit_date=datetime.utcnow(),
                committer_contributor_alias_id=contributors.joe.aliases[0].id,
                committer_contributor_key=contributors.joe.key,
                committer_contributor_name=contributors.joe.name,


                author_contributor_alias_id=contributors.billy.aliases[0].id,
                author_contributor_key=contributors.billy.key,
                author_contributor_name=contributors.billy.name,
                author_team_key=team_a.key,
                author_team_id=team_a.id,


                **self.commits_common_fields(test_repo)
            ),
            # committer_team is assigned
            dict(

                source_commit_id='ZZZZ',
                key=uuid.uuid4().hex,
                commit_date=datetime.utcnow(),
                committer_contributor_alias_id=contributors.joe.aliases[0].id,
                committer_contributor_key=contributors.joe.key,
                committer_contributor_name=contributors.joe.name,
                committer_team_key=team_a.key,
                committer_team_id=team_a.id,

                author_contributor_alias_id=contributors.billy.aliases[0].id,
                author_contributor_key=contributors.billy.key,
                author_contributor_name=contributors.billy.name,


                **self.commits_common_fields(test_repo)
            ),
            dict(

                source_commit_id='AAAAA',
                key=uuid.uuid4().hex,
                commit_date=datetime.utcnow(),
                committer_contributor_alias_id=contributors.joe.aliases[0].id,
                committer_contributor_key=contributors.joe.key,
                committer_contributor_name=contributors.joe.name,
                committer_team_key=team_a.key,
                committer_team_id=team_a.id,

                author_contributor_alias_id=contributors.billy.aliases[0].id,
                author_contributor_key=contributors.billy.key,
                author_contributor_name=contributors.billy.name,
                author_team_key=team_a.key,
                author_team_id=team_a.id,

                **self.commits_common_fields(test_repo)
            ),
            dict(

                source_commit_id='BBBB',
                key=uuid.uuid4().hex,
                commit_date=datetime.utcnow(),
                committer_contributor_alias_id=contributors.joe.aliases[0].id,
                committer_contributor_key=contributors.joe.key,
                committer_contributor_name=contributors.joe.name,
                committer_team_key=team_a.key,
                committer_team_id=team_a.id,

                author_contributor_alias_id=contributors.billy.aliases[0].id,
                author_contributor_key=contributors.billy.key,
                author_contributor_name=contributors.billy.name,
                author_team_key=team_b.key,
                author_team_id=team_b.id,

                **self.commits_common_fields(test_repo)
            )

        ]
        self.create_test_commits(test_commits)

        yield Fixture(
            organization=organization,
            work_items=test_work_items,
            commits=test_commits,
            teams=setup_teams
        )



    def it_works_when_there_are_no_teams_assigned_to_a_commit(self, setup, teardown):
        fixture = setup

        work_items_commits = [
            dict(
                commit_key=UUID(fixture.commits[0]['key']),
                work_item_key=UUID(fixture.work_items[0]['key'])
            )
        ]
        resolved = commands.resolve_teams_for_work_items(fixture.organization.key, work_items_commits)
        assert resolved['success']
        assert resolved['updated'] == 0


    def it_assigns_the_work_item_to_a_team_when_the_commit_author_belongs_to_a_team(self, setup, teardown):
        fixture = setup

        work_items_commits = [
            dict(
                commit_key=UUID(fixture.commits[1]['key']),
                work_item_key=UUID(fixture.work_items[0]['key'])
            )
        ]
        resolved = commands.resolve_teams_for_work_items(fixture.organization.key, work_items_commits)
        assert resolved['success']
        assert resolved['updated'] == 1

    def it_assigns_the_work_item_to_a_team_when_the_committer_belongs_to_a_team(self, setup, teardown):
        fixture = setup

        work_items_commits = [
            dict(
                commit_key=UUID(fixture.commits[2]['key']),
                work_item_key=UUID(fixture.work_items[0]['key'])
            )
        ]
        resolved = commands.resolve_teams_for_work_items(fixture.organization.key, work_items_commits)
        assert resolved['success']
        assert resolved['updated'] == 1

    def it_assigns_the_work_item_to_a_team_when_the_committer__and_author_belongs_to_the_same_team(self, setup, teardown):
        fixture = setup

        work_items_commits = [
            dict(
                commit_key=UUID(fixture.commits[3]['key']),
                work_item_key=UUID(fixture.work_items[0]['key'])
            )
        ]
        resolved = commands.resolve_teams_for_work_items(fixture.organization.key, work_items_commits)
        assert resolved['success']
        assert resolved['updated'] == 1

    def it_assigns_the_work_item_to_multiple_team_when_the_committer_and_author_belongs_to_different_teams(self, setup, teardown):
        fixture = setup

        work_items_commits = [
            dict(
                commit_key=UUID(fixture.commits[4]['key']),
                work_item_key=UUID(fixture.work_items[0]['key'])
            )
        ]
        resolved = commands.resolve_teams_for_work_items(fixture.organization.key, work_items_commits)
        assert resolved['success']
        assert resolved['updated'] == 2

    def it_assigns_the_multiple_work_item_to_multiple_team_when_the_committer_and_author_belongs_to_different_teams(self, setup, teardown):
        fixture = setup

        work_items_commits = [
            dict(
                commit_key=UUID(fixture.commits[4]['key']),
                work_item_key=UUID(fixture.work_items[0]['key'])
            ),
            dict(
                commit_key=UUID(fixture.commits[4]['key']),
                work_item_key=UUID(fixture.work_items[1]['key'])
            )
        ]
        resolved = commands.resolve_teams_for_work_items(fixture.organization.key, work_items_commits)
        assert resolved['success']
        assert resolved['updated'] == 4

    @pytest.fixture()
    def teardown(self):
        yield

        db.connection().execute("delete from analytics.work_items_teams")
        db.connection().execute("delete from analytics.work_items_commits")
        db.connection().execute("delete from analytics.work_item_delivery_cycles")
        db.connection().execute("delete from analytics.work_items")
        db.connection().execute("delete from analytics.work_items_sources")

        db.connection().execute("delete from analytics.commits")
        db.connection().execute("delete from analytics.contributor_aliases")
        db.connection().execute("delete from analytics.organizations_contributors")
        db.connection().execute("delete from analytics.contributors")
        db.connection().execute("delete from analytics.teams")