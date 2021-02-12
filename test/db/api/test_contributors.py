# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.contributors import *

from polaris.analytics.db.api import update_contributor_for_contributor_aliases
from polaris.analytics.db.model import ContributorAlias


class TestUpdateContributorForContributorAliases:

    def it_points_the_contributor_alias_to_the_new_contributor(self, setup_commits_for_contributor_updates):
        result = update_contributor_for_contributor_aliases(
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
        result = update_contributor_for_contributor_aliases(
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
        result = update_contributor_for_contributor_aliases(
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
        result = update_contributor_for_contributor_aliases(
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
        result = update_contributor_for_contributor_aliases(
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
        result = update_contributor_for_contributor_aliases(
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
        result = update_contributor_for_contributor_aliases(
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
        result = update_contributor_for_contributor_aliases(
            joe_contributor_key,
            dict(
                contributor_name='Joe 2.0'
            )
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.contributors where key='{joe_contributor_key}' and name='Joe 2.0'").scalar() == 1

    def it_sets_robot_true_for_contributor_aliases_excluded_from_analysis(self, setup_commits_for_contributor_updates):
        result = update_contributor_for_contributor_aliases(
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
        result = update_contributor_for_contributor_aliases(
            joe_contributor_key,
            dict(
                contributor_alias_keys=[joe_alt_contributor_key]
            )
        )
        assert result['success']
        # Unlink now
        result = update_contributor_for_contributor_aliases(
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
