# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.contributors import *
from test.fixtures.teams import *
from test.fixtures.graphql import get_date

from datetime import datetime, timedelta
from polaris.analytics.db.api import update_contributor
from polaris.analytics.db.model import ContributorAlias, WorkItem, WorkItemsSource, Repository, Organization, Team
from polaris.analytics.db.commands import assign_contributor_commits_to_teams

class TestUpdateContributorForContributorAliases:

    def it_points_the_contributor_alias_to_the_new_contributor(self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[joe_alt_contributor_key]
            )
        )
        assert result['success']

        with db.orm_session() as session:
            joe_alt = ContributorAlias.find_by_contributor_alias_key(session, joe_alt_contributor_key)
            assert joe_alt.contributor.key.hex == joe_contributor_key

    def it_attributes_all_commits_authored_by_the_alias_to_the_new_contributor(
            self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[joe_alt_contributor_key]
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.commits where author_contributor_key='{joe_contributor_key}' and author_contributor_name='Joe Blow'"
        ).scalar() == 2

    def it_removes_attributions_for_all_commits_authored_by_the_alias_to_the_old_contributor(
            self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[joe_alt_contributor_key]
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.commits where author_contributor_key='{joe_alt_contributor_key}'"
        ).scalar() == 0

    def it_attributes_all_commits_committed_by_the_alias_to_the_new_contributor(
            self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[billy_contributor_key]
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.commits where committer_contributor_key='{joe_contributor_key}' and committer_contributor_name='Joe Blow'"
        ).scalar() == 2

    def it_removes_attributions_for_all_commits_committed_by_the_alias_to_the_old_contributor(
            self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[billy_contributor_key]
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.commits where committer_contributor_key='{billy_contributor_key}'"
        ).scalar() == 0

    # All contributions to repositories under the old alias are now to be attributed to the new contributor

    def it_removes_the_old_contributor_from_the_repositories_they_contributed_to(
            self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[joe_alt_contributor_key]
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(contributors.id) "
            f"from analytics.repositories_contributor_aliases "
            f"inner join analytics.contributors on repositories_contributor_aliases.contributor_id = contributors.id "
            f"where contributors.key='{joe_alt_contributor_key}'"
        ).scalar() == 0

    def it_attributes_all_repositories_to_the_new_contributor(
            self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[joe_alt_contributor_key]
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(contributors.id) "
            f"from analytics.repositories_contributor_aliases "
            f"inner join analytics.contributors on repositories_contributor_aliases.contributor_id = contributors.id "
            f"where contributors.key='{joe_contributor_key}'"
        ).scalar() == 2

    def it_updates_name_of_contributor(self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_name='Joe 2.0'
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.contributors where key='{joe_contributor_key}' and name='Joe 2.0'").scalar() == 1

    def it_sets_robot_true_for_contributor_aliases_excluded_from_analysis(self, setup_commits_for_contributor_updates):
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[joe_alt_contributor_key],
                excluded_from_analysis=True
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(contributor_aliases.id) "
            f"from analytics.contributor_aliases "
            f"join analytics.contributors on contributor_aliases.contributor_id = contributors.id "
            f"where contributors.key='{joe_contributor_key}' and contributor_aliases.robot=true"
        ).scalar() == 2

    def it_unlinks_contributor_alias_from_a_contributor(self, setup_commits_for_contributor_updates):
        # Merge first
        result = update_contributor(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[joe_alt_contributor_key]
            )
        )
        assert result['success']
        # Unlink now
        result = update_contributor(
            joe_contributor_key,
            dict(
                unlink_contributor_alias_keys=[joe_alt_contributor_key]
            )
        )
        assert result['success']
        # Contributor_id for alias is set back to original id corresponding to same alias
        assert db.connection().execute(
            f"select count(contributor_aliases.id) from analytics.contributor_aliases "
            f"join analytics.contributors on contributor_aliases.key=contributors.key "
            f"where contributor_aliases.key='{joe_alt_contributor_key}'"
            f"and contributor_aliases.contributor_id=contributors.id").scalar() == 1
        # Commits as author are attributed back to the alias key
        assert db.connection().execute(
            f"select count(id) from analytics.commits where author_contributor_key='{joe_contributor_key}'"
        ).scalar() == 1
        assert db.connection().execute(
            f"select count(id) from analytics.commits where author_contributor_key='{joe_alt_contributor_key}' and author_contributor_name='Joe G. Blow'"
        ).scalar() == 1
        # repositories
        assert db.connection().execute(
            f"select count(contributors.id) "
            f"from analytics.repositories_contributor_aliases "
            f"inner join analytics.contributors on repositories_contributor_aliases.contributor_id = contributors.id "
            f"where contributors.key='{joe_alt_contributor_key}'"
        ).scalar() == 1
        assert db.connection().execute(
            f"select count(contributors.id) "
            f"from analytics.repositories_contributor_aliases "
            f"inner join analytics.contributors on repositories_contributor_aliases.contributor_id = contributors.id "
            f"where contributors.key='{joe_contributor_key}'"
        ).scalar() == 1


class TestAssignContributorCommitsToTeams:

    @pytest.yield_fixture()
    def setup(self, setup_commits_for_contributor_updates, setup_teams):
        yield Fixture(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            team_a=setup_teams.team_a,
            team_b=setup_teams.team_b,
            joe=joe_contributor_key,
            bill=billy_contributor_key
        )

    class TestInitialAssignment:

        def it_returns_the_right_response(self, setup):
            fixture = setup

            result = assign_contributor_commits_to_teams(
                fixture.organization_key,
                [
                    dict(
                        contributor_key=joe_contributor_key,
                        new_team_key=fixture.team_a['key'],
                        initial_assignment=True
                    )
                ]
            )
            assert result['success']
            assert result['update_count'] == 1

            assert db.connection().execute(
                "select count(id) from analytics.commits where author_team_key is not null"
            ).scalar() == 1

        def it_assigns_author_team_key_to_authors_who_have_teams_assigned(self, setup):
            fixture = setup

            result = assign_contributor_commits_to_teams(
                fixture.organization_key,
                [
                    dict(
                        contributor_key=joe_contributor_key,
                        new_team_key=fixture.team_a['key'],
                        initial_assignment=True
                    )
                ]
            )
            assert result['success']
            assert result['update_count'] == 1

            assert db.connection().execute(
                "select count(id) from analytics.commits where author_team_key is not null"
            ).scalar() == 1

        def it_does_not_assign_contributor_team_key_if_thecontributor_is_not_a_committer(self, setup):
            fixture = setup

            result = assign_contributor_commits_to_teams(
                fixture.organization_key,
                [
                    dict(
                        contributor_key=joe_contributor_key,
                        new_team_key=fixture.team_a['key'],
                        initial_assignment=True
                    )
                ]
            )
            assert result['success']
            assert result['update_count'] == 1

            assert db.connection().execute(
                "select committer_team_key from analytics.commits where author_team_key is not null"
            ).scalar() is None


        def it_assigns_committer_team_key_to_committers_who_have_teams_assigned(self, setup):
            fixture = setup

            result = assign_contributor_commits_to_teams(
                fixture.organization_key,
                [
                    dict(
                        contributor_key=billy_contributor_key,
                        new_team_key=fixture.team_a['key'],
                        initial_assignment=True
                    )
                ]
            )
            assert result['success']
            assert result['update_count'] == 1

            assert db.connection().execute(
                "select count(id) from analytics.commits where committer_team_key is not null"
            ).scalar() == 2

        def it_does_not_assign_author_team_key_if_the_contributor_is_not_an_author(self, setup):
            fixture = setup

            result = assign_contributor_commits_to_teams(
                fixture.organization_key,
                [
                    dict(
                        contributor_key=billy_contributor_key,
                        new_team_key=fixture.team_a['key'],
                        initial_assignment=True
                    )
                ]
            )
            assert result['success']
            assert result['update_count'] == 1

            assert db.connection().execute(
                "select author_team_key from analytics.commits where committer_team_key is not null"
            ).scalar() is None

    class TestWorkItemTeamAssignments:

        def work_item_source_common(self):
            return dict(
                key=uuid.uuid4().hex,
                name='Rails Project',
                integration_type='github',
                work_items_source_type='repository_issues',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=rails_organization_key,
                source_id=str(uuid.uuid4())
            )

        def work_items_common(self):
            return dict(
                work_item_type='issue',
                is_bug=True,
                is_epic=False,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow() - timedelta(days=7),
                updated_at=datetime.utcnow() - timedelta(days=6),
                state='open',
                source_id=str(uuid.uuid4()),
                parent_id=None
            )

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup
            with db.orm_session() as session:
                organization = Organization.find_by_organization_key(session, rails_organization_key)
                work_item_source = WorkItemsSource(
                    organization_key=rails_organization_key,
                    **self.work_item_source_common()
                )
                organization.work_items_sources.append(work_item_source)
                new_work_items = [
                    WorkItem(
                        key=uuid.uuid4().hex,
                        name='Issue 1',
                        display_id='1000',

                        **self.work_items_common()
                    ),
                    WorkItem(
                        key=uuid.uuid4().hex,
                        name='Issue 2',
                        display_id='2000',

                        **self.work_items_common()
                    ),
                    WorkItem(
                        key=uuid.uuid4().hex,
                        name='Issue 3',
                        display_id='2000',

                        **self.work_items_common()
                    ),
                ]
                work_item_source.work_items.extend(
                    new_work_items
                )
                repo = Repository.find_by_repository_key(session, rails_repository_key)


                new_work_items[0].commits.append(repo.commits[0])
                new_work_items[1].commits.append(repo.commits[1])



            yield fixture

        def it_assigns_the_work_items_for_a_commit_to_the_team_based_on_author(self, setup):
            fixture = setup

            # Joe is author on commit 0 only and that is associated with one work item
            # so when we associate joe with a team, we expect the team to have one work item

            result = assign_contributor_commits_to_teams(
                fixture.organization_key,
                [
                    dict(
                        contributor_key=joe_contributor_key,
                        new_team_key=fixture.team_a['key'],
                        initial_assignment=True
                    )
                ]
            )
            assert result['success']
            assert result['update_count'] == 1

            with db.orm_session() as session:
                team = Team.find_by_key(session, fixture.team_a['key'])
                assert len(team.work_items) == 1

        def it_assigns_the_work_items_for_a_commit_to_the_team_based_on_committer(self, setup):
            fixture = setup

            # Billy is committer on both commits only and each is associated with one work item
            # so when we associate joe with a team, we expect the team to have two work items

            result = assign_contributor_commits_to_teams(
                fixture.organization_key,
                [
                    dict(
                        contributor_key=billy_contributor_key,
                        new_team_key=fixture.team_a['key'],
                        initial_assignment=True
                    )
                ]
            )
            assert result['success']
            assert result['update_count'] == 1

            with db.orm_session() as session:
                team = Team.find_by_key(session, fixture.team_a['key'])
                assert len(team.work_items) == 2
