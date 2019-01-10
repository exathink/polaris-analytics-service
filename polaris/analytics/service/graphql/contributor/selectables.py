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

from sqlalchemy import select, func, bindparam, distinct, and_, cast, Text, between, extract
from polaris.graphql.interfaces import NamedNode

from datetime import datetime, timedelta
from polaris.utils.datetime_utils import time_window

from polaris.analytics.db.model import repositories, repositories_contributor_aliases, contributors, \
    contributor_aliases, commits

from ..interfaces import CommitSummary, RepositoryCount, CommitInfo, CommitCount, CumulativeCommitCount
from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_time_window_filters


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


class ContributorRepositoriesNodes:
    interfaces = (NamedNode, CommitSummary)

    @staticmethod
    def selectable(**kwargs):
        return select([
            repositories.c.id,
            func.min(repositories.c.name).label('name'),
            func.min(cast(repositories.c.key, Text)).label('key'),
            func.min(repositories_contributor_aliases.c.earliest_commit).label('earliest_commit'),
            func.max(repositories_contributor_aliases.c.latest_commit).label('latest_commit'),
            func.sum(repositories_contributor_aliases.c.commit_count).label('commit_count')
        ]).select_from(
            contributors.join(
                repositories_contributor_aliases, repositories_contributor_aliases.c.contributor_id == contributors.c.id
            ).join (
                repositories, repositories_contributor_aliases.c.repository_id == repositories.c.id
            )
        ).where(
            contributors.c.key == bindparam('key')
        ).group_by(
            repositories.c.id
        )


class ContributorRecentlyActiveRepositoriesNodes:
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def selectable(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        query = select([
            repositories.c.id,
            func.min(cast(repositories.c.key, Text)).label('key'),
            func.min(repositories.c.name).label('name'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            commits.join(
                repositories
            )
        ).where(
            and_(
                commits.c.author_contributor_key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            repositories.c.id
        ).order_by(
            func.count(commits.c.id).desc()
        )

        if kwargs.get('top'):
            query = query.limit(kwargs['top'])

        return query


class ContributorCommitNodes:
    interface = CommitInfo

    @staticmethod
    def selectable(**kwargs):
        select_stmt = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            commits.join(
                repositories
            )
        ).where(
            commits.c.author_contributor_key == bindparam('key')
        )
        return commits_connection_apply_time_window_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(contributor_commit_nodes, **kwargs):
        return [contributor_commit_nodes.c.commit_date.desc()]


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

    @staticmethod
    def sort_order(contributor_summary, **kwargs):
        return [contributor_summary.c.commit_count.desc()]


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

class ContributorCumulativeCommitCount:

    interface = CumulativeCommitCount

    @staticmethod
    def selectable(**kwargs):
        commit_counts = select([
            extract('year', commits.c.commit_date).label('year'),
            extract('week', commits.c.commit_date).label('week'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            commits
        ).where(
            commits.c.author_contributor_key == bindparam('key')
        ).group_by(
            extract('year', commits.c.commit_date),
            extract('week', commits.c.commit_date)
        ).alias()

        return select([
            commit_counts.c.year,
            commit_counts.c.week,
            func.sum(commit_counts.c.commit_count).over(order_by=[
                commit_counts.c.year,
                commit_counts.c.week
            ]).label('cumulative_commit_count')
        ])

