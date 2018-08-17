# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func, bindparam, and_, distinct

from polaris.graphql.interfaces import NamedNode
from polaris.repos.db.model import repositories, organizations
from polaris.repos.db.schema import contributors, contributor_aliases, repositories_contributor_aliases
from ..interfaces import CommitSummary, ContributorCount, OrganizationRef


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
