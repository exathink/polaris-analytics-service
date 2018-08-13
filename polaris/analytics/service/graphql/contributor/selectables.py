# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, func, bindparam, or_, distinct

from polaris.graphql.interfaces import NamedNode
from polaris.repos.db.model import repositories
from polaris.repos.db.schema import repositories, repositories_contributor_aliases, contributors, commits, contributor_aliases
from ..interfaces import CommitSummary, RepositoryCount


class ContributorNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            contributors.c.id,
            contributors.c.key,
            contributors.c.name
        ]).select_from(
            contributors
        ).where(contributors.c.key == bindparam('key'))





class ContributorsCommitSummary:
    interface = CommitSummary

    @staticmethod
    def selectable(contributor_nodes, **kwargs):
        return select([
            contributor_nodes.c.id,
            func.count(distinct(commits.c.id)).label('commit_count'),
            func.min(commits.c.commit_date).label('earliest_commit'),
            func.max(commits.c.commit_date).label('latest_commit')

        ]).select_from(
            contributor_nodes.outerjoin(
                contributors, contributor_nodes.c.id == contributors.c.id,
            ).outerjoin(
                contributor_aliases, contributors.c.id == contributor_aliases.c.contributor_id
            ).outerjoin(
                commits,
                or_(
                    commits.c.author_alias_id == contributor_aliases.c.id,
                    commits.c.committer_alias_id == contributor_aliases.c.id
                )
            ).outerjoin(
                repositories, repositories.c.id == commits.c.repository_id
            )
        ).group_by(contributor_nodes.c.id)

class ContributorsRepositoryCount:
    interface = RepositoryCount

    @staticmethod
    def selectable(contributor_nodes, **kwargs):
        return select([
            contributor_nodes.c.id,
            func.count(distinct(repositories_contributor_aliases.c.repository_id)).label('repository_count')
        ]).select_from(
            contributor_nodes.outerjoin(
                contributors, contributor_nodes.c.id == contributors.c.id,
            ).outerjoin(
                contributor_aliases, contributors.c.id == contributor_aliases.c.contributor_id
            ).outerjoin(
                repositories_contributor_aliases, contributor_aliases.c.id == repositories_contributor_aliases.c.contributor_alias_id
            )
        ).group_by(contributor_nodes.c.id)


