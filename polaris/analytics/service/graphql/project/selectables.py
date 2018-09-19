# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func, bindparam, distinct, and_, cast, Text, between, extract
from polaris.graphql.utils import nulls_to_zero
from datetime import datetime, timedelta
from polaris.utils.datetime_utils import time_window

from polaris.graphql.interfaces import NamedNode
from polaris.repos.db.model import projects, projects_repositories, organizations
from polaris.repos.db.schema import repositories, contributors, \
    contributor_aliases, repositories_contributor_aliases, commits

from ..interfaces import \
    CommitSummary, ContributorCount, RepositoryCount, OrganizationRef, CommitCount, \
    CumulativeCommitCount, CommitInfo

from ..commit.column_expressions import commit_info_columns


class ProjectNode:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            projects.c.id,
            projects.c.project_key.label('key'),
            projects.c.name
        ]).select_from(
            projects
        ).where(projects.c.project_key == bindparam('key'))


class ProjectRepositoriesNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name
        ]).select_from(
            projects.join(
                projects_repositories
            ).join(
                repositories
            )
        ).where(projects.c.project_key == bindparam('key'))


class ProjectRecentlyActiveRepositoriesNodes:
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def selectable(**kwargs):
        now = datetime.utcnow()
        window = time_window(begin=now - timedelta(days=kwargs.get('days', 7)), end=now)

        return select([
            repositories.c.id,
            func.min(cast(repositories.c.key, Text)).label('key'),
            func.min(repositories.c.name).label('name'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits
            )
        ).where(
            and_(
                projects.c.project_key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            repositories.c.id
        )

    @staticmethod
    def sort_order(project_recently_active_repositories, **kwargs):
        return [project_recently_active_repositories.c.commit_count.desc()]


class ProjectCommitNodes:
    interface = CommitInfo

    @staticmethod
    def selectable(**kwargs):
        select_stmt = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join (
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(commits)
        ).where(
            projects.c.project_key == bindparam('key')
        )
        if 'days' in kwargs and kwargs['days'] > 0:
            now = datetime.utcnow()
            commit_window_start = now - timedelta(days=kwargs['days'])
            select_stmt = select_stmt.where(
                commits.c.commit_date >= commit_window_start
            )

        return select_stmt

    @staticmethod
    def sort_order(project_commit_nodes, **kwargs):
        return [project_commit_nodes.c.commit_date.desc()]


class ProjectContributorNodes:
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
                    repositories.join(
                        projects_repositories
                    ).join(
                        projects
                    )
                )
            )
        ).where(
            and_(
                projects.c.project_key == bindparam('key'),
                repositories_contributor_aliases.c.robot == False
            )
        ).distinct()


class ProjectRecentlyActiveContributorNodes:
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def selectable(**kwargs):
        now = datetime.utcnow()
        window = time_window(begin=now - timedelta(days=kwargs.get('days', 7)), end=now)

        return select([
            commits.c.author_contributor_key.label('key'),
            func.min(commits.c.author_contributor_name).label('name'),
            func.count(distinct(commits.c.id)).label('commit_count')

        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits
            ).join(
                contributor_aliases, commits.c.author_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                projects.c.project_key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end),
                contributor_aliases.c.robot == False
            )
        ).group_by(
            commits.c.author_contributor_key
        )

    @staticmethod
    def sort_order(recently_active_contributors, **kwargs):
        return [recently_active_contributors.c.commit_count.desc()]


class ProjectCumulativeCommitCount:

    interface = CumulativeCommitCount

    @staticmethod
    def selectable(**kwargs):
        commit_counts = select([
            extract('year', commits.c.commit_date).label('year'),
            extract('week', commits.c.commit_date).label('week'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            projects.c.project_key == bindparam('key')
        ).group_by(
            extract('year', commits.c.commit_date),
            extract('week', commits.c.commit_date)
        ).alias('weekly_commit_counts')

        return select([
            commit_counts.c.year,
            commit_counts.c.week,
            func.sum(commit_counts.c.commit_count).over(order_by=[
                commit_counts.c.year,
                commit_counts.c.week
            ]).label('cumulative_commit_count')
        ])


class ProjectsCommitSummary:
    interface = CommitSummary

    @staticmethod
    def selectable(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            project_nodes.outerjoin(
                projects_repositories, project_nodes.c.id == projects_repositories.c.project_id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            )
        ).group_by(project_nodes.c.id)

    @staticmethod
    def sort_order(projects_commit_summary, **kwargs):
        return [nulls_to_zero(projects_commit_summary.c.commit_count).desc()]


class ProjectsContributorCount:
    interface = ContributorCount

    @staticmethod
    def selectable(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            func.count(distinct(repositories_contributor_aliases.c.contributor_id)).label('contributor_count')
        ]).select_from(
            project_nodes.outerjoin(
                projects_repositories, projects_repositories.c.project_id == project_nodes.c.id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            )
        ).where(
            repositories_contributor_aliases.c.robot == False
        ).group_by(project_nodes.c.id)

class ProjectsRepositoryCount:
    interface = RepositoryCount

    @staticmethod
    def selectable(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            func.count(repositories.c.id).label('repository_count')

        ]).select_from(
            project_nodes.outerjoin(
                projects_repositories, project_nodes.c.id == projects_repositories.c.project_id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            )
        ).group_by(project_nodes.c.id)

class ProjectsOrganizationRef:
    interface = OrganizationRef

    @staticmethod
    def selectable(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            organizations.c.organization_key.label('organization_key'),
            organizations.c.name.label('organization_name')

        ]).select_from(
            project_nodes.outerjoin(
                projects, project_nodes.c.id == projects.c.id
            ).outerjoin(
                organizations, projects.c.organization_id == organizations.c.id
            )
        )