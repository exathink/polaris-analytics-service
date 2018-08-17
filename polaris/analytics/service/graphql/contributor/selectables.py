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

from sqlalchemy import select, func, bindparam, distinct, and_

from polaris.graphql.interfaces import NamedNode
from polaris.repos.db.model import repositories
from polaris.repos.db.schema import repositories, repositories_contributor_aliases, contributors, contributor_aliases
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
    def repository_level_of_detail(contributor_repository_nodes, **kwargs):
        return select([
            contributor_repository_nodes.c.id,
            func.sum(repositories_contributor_aliases.c.commit_count).label('commit_count'),
            func.min(repositories_contributor_aliases.c.earliest_commit).label('earliest_commit'),
            func.max(repositories_contributor_aliases.c.latest_commit).label('latest_commit')

        ]).select_from(
            contributor_repository_nodes.outerjoin(
                repositories_contributor_aliases,
                and_(
                    repositories_contributor_aliases.c.repository_id == contributor_repository_nodes.c.repository_id,
                    repositories_contributor_aliases.c.contributor_id == contributor_repository_nodes.c.id
                )
            )
        ).group_by(contributor_repository_nodes.c.id)


    @staticmethod
    def contributor_level_of_detail(contributor_nodes, **kwargs):
        return select([
            contributor_nodes.c.id,
            func.sum(repositories_contributor_aliases.c.commit_count).label('commit_count'),
            func.min(repositories_contributor_aliases.c.earliest_commit).label('earliest_commit'),
            func.max(repositories_contributor_aliases.c.latest_commit).label('latest_commit')

        ]).select_from(
            contributor_nodes.outerjoin(
                contributors, contributor_nodes.c.id == contributors.c.id,
            ).outerjoin(
                contributor_aliases, contributors.c.id == contributor_aliases.c.contributor_id
            ).outerjoin(
                repositories_contributor_aliases,
                repositories_contributor_aliases.c.contributor_alias_id == contributor_aliases.c.id
            ).outerjoin(
                repositories, repositories.c.id == repositories_contributor_aliases.c.repository_id
            )
        ).group_by(contributor_nodes.c.id)

    @staticmethod
    def selectable(contributor_nodes, **kwargs):
        level_of_detail = kwargs.get('level_of_detail')
        if level_of_detail == 'repository':
            return ContributorsCommitSummary.repository_level_of_detail(contributor_nodes, **kwargs)
        else:
            return ContributorsCommitSummary.contributor_level_of_detail(contributor_nodes, **kwargs)




class ContributorsRepositoryCount:
    interface = RepositoryCount

    @staticmethod
    def contributor_level_of_detail(contributor_nodes, **kwargs):
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

    @staticmethod
    def repository_level_of_detail(contributor_repository_nodes, **kwargs):
        return select([
            contributor_repository_nodes.c.id,
            func.count(distinct(contributor_repository_nodes.c.repository_id)).label('repository_count')
        ]).select_from(
            contributor_repository_nodes
        ).group_by(contributor_repository_nodes.c.id)


    @staticmethod
    def selectable(contributor_nodes, **kwargs):
        level_of_detail = kwargs.get('level_of_detail')
        if level_of_detail == 'repository':
            return ContributorsRepositoryCount.repository_level_of_detail(contributor_nodes, **kwargs)
        else:
            return ContributorsRepositoryCount.contributor_level_of_detail(contributor_nodes, **kwargs)