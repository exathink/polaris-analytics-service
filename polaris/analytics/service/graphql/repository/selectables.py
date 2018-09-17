# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func, bindparam, and_, distinct, extract
from polaris.graphql.utils import nulls_to_zero, is_paging
from polaris.graphql.interfaces import NamedNode
from polaris.repos.db.model import repositories, organizations
from polaris.repos.db.schema import contributors, commits, repositories_contributor_aliases
from ..interfaces import CommitSummary, ContributorCount, OrganizationRef, CommitInfo, CumulativeCommitCount
from ..commit.column_expressions import commit_info_columns
from datetime import datetime, timedelta

class RepositoryNode:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name
        ]).select_from(
            repositories
        ).where(
            repositories.c.key == bindparam('key')
        )


class RepositoryCommitNodes:
    interface = CommitInfo

    @staticmethod
    def selectable(**kwargs):
        select_stmt = select([
            *commit_info_columns()
        ]).select_from(
            repositories.join(commits)
        ).where(
            repositories.c.key == bindparam('key')
        )
        if 'days' in kwargs and kwargs['days'] > 0:
            now = datetime.utcnow()
            commit_window_start = now - timedelta(days=kwargs['days'])
            select_stmt = select_stmt.where(
                commits.c.commit_date >= commit_window_start
            )

        return select_stmt

    @staticmethod
    def sort_order(repository_commit_nodes, **kwargs):
        return [repository_commit_nodes.c.commit_date.desc()]


class RepositoryContributorNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            contributors.c.id,
            contributors.c.key,
            contributors.c.name,
            repositories_contributor_aliases.c.repository_id
        ]).select_from(
            contributors.join(
                    repositories_contributor_aliases.join(
                    repositories
                )
            )
        ).where(
            and_(
                repositories.c.key == bindparam('key'),
                repositories_contributor_aliases.c.robot == False
            )
        ).distinct()


class RepositoryCumulativeCommitCount:

    interface = CumulativeCommitCount

    @staticmethod
    def selectable(**kwargs):
        commit_counts = select([
            extract('year', commits.c.commit_date).label('year'),
            extract('week', commits.c.commit_date).label('week'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            repositories.join(commits)
        ).where(
            repositories.c.key == bindparam('key')
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





class RepositoriesCommitSummary:
    interface = CommitSummary

    @staticmethod
    def selectable(repositories_nodes, **kwargs):
        return select([
            repositories_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            repositories_nodes.outerjoin(
                repositories,
                repositories_nodes.c.id == repositories.c.id
            )
        ).group_by(repositories_nodes.c.id)

    @staticmethod
    def sort_order(repositories_commit_summary, **kwargs):
        return [nulls_to_zero(repositories_commit_summary.c.commit_count).desc()]


class RepositoriesContributorCount:
    interface = ContributorCount

    @staticmethod
    def selectable(repositories_nodes, **kwargs):
        return select([
            repositories_nodes.c.id,
            func.count(distinct(repositories_contributor_aliases.c.contributor_id)).label('contributor_count')
        ]).select_from(
            repositories_nodes.outerjoin(
                repositories, repositories_nodes.c.id == repositories.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            )
        ).where(
            repositories_contributor_aliases.c.robot == False
        ).group_by(repositories_nodes.c.id)

class RepositoriesOrganizationRef:
    interface = OrganizationRef

    @staticmethod
    def selectable(repositories_nodes, **kwargs):
        return select([
            repositories_nodes.c.id,
            organizations.c.organization_key,
            organizations.c.name.label('organization_name')

        ]).select_from(
            repositories_nodes.outerjoin(
                repositories,
                repositories_nodes.c.id == repositories.c.id
            ).outerjoin(
                organizations, repositories.c.organization_id == organizations.c.id
            )
        )
