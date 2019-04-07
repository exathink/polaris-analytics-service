# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.analytics.db.model import Contributor, ContributorAlias
from polaris.analytics.db.model import contributor_aliases, commits, repositories_contributor_aliases
from polaris.utils.exceptions import ProcessingException


def update_contributor_for_contributor_alias(session, contributor, contributor_alias_key):
    contributor_alias = ContributorAlias.find_by_contributor_alias_key(session, contributor_alias_key)
    if contributor_alias:
        # set the new contributor_id for this alias
        session.connection().execute(
            contributor_aliases.update().where(
                contributor_aliases.c.id == contributor_alias.id
            ).values(
                contributor_id=contributor.id
            )
        )
        # rewrite denormalized author_contributor info on commits referencing this alias
        session.connection().execute(
            commits.update().where(
                commits.c.author_contributor_alias_id == contributor_alias.id
            ).values(
                author_contributor_key=contributor.key,
                author_contributor_name=contributor.name
            )
        )
        # rewrite denormalized commiter_contributor info on commits referencing this alias
        session.connection().execute(
            commits.update().where(
                commits.c.committer_contributor_alias_id == contributor_alias.id
            ).values(
                committer_contributor_key=contributor.key,
                committer_contributor_name=contributor.name
            )
        )

        # rewrite the denormalized contributor info on repository_contributor aliases
        session.connection().execute(
            repositories_contributor_aliases.update().where(
                repositories_contributor_aliases.c.contributor_alias_id == contributor_alias.id
            ).values(
                contributor_id=contributor.id
            )
        )


    else:
        raise ProcessingException(f'Could not find contributor alias with key {contributor_alias_key}')


def update_contributor_for_contributor_aliases(session, organization_key, contributor_key, contributor_alias_keys):
    contributor = Contributor.find_by_contributor_key(session, contributor_key)
    if contributor:
        for alias_key in contributor_alias_keys:
            update_contributor_for_contributor_alias(session, contributor, alias_key)

        return dict(
            updated_alias_keys=contributor_alias_keys
        )
    else:
        raise ProcessingException(f"Contributor with key: {contributor_key} was not found")
