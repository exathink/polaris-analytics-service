# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func, bindparam

from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from ..selectables import select_contributor_summary

from polaris.repos.db.model import projects, projects_repositories
from polaris.repos.db.schema import repositories, contributor_aliases, repositories_contributor_aliases

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
        ).where(projects.c.project_key == bindparam('project_key'))

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
        ).where(projects.c.project_key == bindparam('project_key'))

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


class ProjectsContributorSummary:
    interface = ContributorSummary

    @staticmethod
    def selectable(project_nodes, **kwargs):
        contributor_nodes = select([
            project_nodes.c.id,
            contributor_aliases.c.id.label('ca_id'),
            contributor_aliases.c.contributor_key
        ]).select_from(
            project_nodes.outerjoin(
                projects_repositories, projects_repositories.c.project_id == project_nodes.c.id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            ).outerjoin(
                contributor_aliases, contributor_aliases.c.id == repositories_contributor_aliases.c.contributor_alias_id
            )
        ).where(
            contributor_aliases.c.robot == False
        ).cte('contributor_nodes')

        return select_contributor_summary(contributor_nodes)