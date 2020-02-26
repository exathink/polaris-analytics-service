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
            rails_organization_key,
            joe_contributor_key,
            [joe_alt_contributor_key]
        )
        assert result['success']

        with db.orm_session() as session:
            joe_alt = ContributorAlias.find_by_contributor_alias_key(session, joe_alt_contributor_key)
            assert joe_alt.contributor.key.hex == joe_contributor_key


    def it_attributes_all_commits_authored_by_the_alias_to_the_new_contributor(
            self, setup_commits_for_contributor_updates):
        result = update_contributor_for_contributor_aliases(
            rails_organization_key,
            joe_contributor_key,
            [joe_alt_contributor_key]
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.commits where author_contributor_key='{joe_contributor_key}'"
        ).scalar() == 2

    def it_removes_attributions_for_all_commits_authored_by_the_alias_to_the_old_contributor(
            self,setup_commits_for_contributor_updates):
        result = update_contributor_for_contributor_aliases(
            rails_organization_key,
            joe_contributor_key,
            [joe_alt_contributor_key]
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.commits where author_contributor_key='{joe_alt_contributor_key}'"
        ).scalar() == 0


    def it_attributes_all_commits_committed_by_the_alias_to_the_new_contributor(
            self, setup_commits_for_contributor_updates):
        result = update_contributor_for_contributor_aliases(
            rails_organization_key,
            joe_contributor_key,
            [billy_contributor_key]
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.commits where committer_contributor_key='{joe_contributor_key}'"
        ).scalar() == 2

    def it_removes_attributions_for_all_commits_committed_by_the_alias_to_the_old_contributor(
            self,setup_commits_for_contributor_updates):
        result = update_contributor_for_contributor_aliases(
            rails_organization_key,
            joe_contributor_key,
            [billy_contributor_key]
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.commits where committer_contributor_key='{billy_contributor_key}'"
        ).scalar() == 0

    # All contributions to repositories under the old alias are now to be attributed to the new contributor

    def it_removes_the_old_contributor_from_the_repositories_they_contributed_to(
            self, setup_commits_for_contributor_updates):
        result = update_contributor_for_contributor_aliases(
            rails_organization_key,
            joe_contributor_key,
            [joe_alt_contributor_key]
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
            rails_organization_key,
            joe_contributor_key,
            [joe_alt_contributor_key]
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(contributors.id) "
            f"from analytics.repositories_contributor_aliases "
            f"inner join analytics.contributors on repositories_contributor_aliases.contributor_id = contributors.id "
            f"where contributors.key='{joe_contributor_key}'"
        ).scalar() == 2
