# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func, bindparam, distinct, and_

from polaris.graphql.interfaces import NamedNode
from polaris.repos.db.model import organizations, projects, repositories
from polaris.repos.db.schema import repositories, contributors, contributor_aliases, \
    repositories_contributor_aliases
from ..interfaces import CommitSummary, ContributorCount, ProjectCount, RepositoryCount


class OrganizationNode:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            organizations.c.id,
            organizations.c.organization_key.label('key'),
            organizations.c.name,

        ]).select_from(
            organizations
        ).where(organizations.c.organization_key == bindparam('key'))


class OrganizationProjectsNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            projects.c.id,
            projects.c.project_key.label('key'),
            projects.c.name
        ]).select_from(
            projects.join(
                organizations
            )
        ).where(organizations.c.organization_key == bindparam('key'))


class OrganizationRepositoriesNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name
        ]).select_from(
            repositories.join(
                organizations
            )
        ).where(organizations.c.organization_key == bindparam('key'))


class OrganizationContributorNodes:
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
                        organizations
                    )
                )
            )
        ).where(
            and_(
                organizations.c.organization_key == bindparam('key'),
                repositories_contributor_aliases.c.robot == False
            )
        ).distinct()



class OrganizationsCommitSummary:
    interface = CommitSummary

    @staticmethod
    def selectable(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            organization_nodes.outerjoin(repositories, organization_nodes.c.id == repositories.c.organization_id)
        ).group_by(organization_nodes.c.id)

    @staticmethod
    def sort_order(organizations_commit_summary, **kwargs):
        return [organizations_commit_summary.c.commit_count.desc()]


class OrganizationsContributorCount:
    interface = ContributorCount

    @staticmethod
    def selectable(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.count(distinct(repositories_contributor_aliases.c.contributor_id)).label('contributor_count')
        ]).select_from(
            organization_nodes.outerjoin(
                repositories, repositories.c.organization_id == organization_nodes.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            )
        ).where(
            repositories_contributor_aliases.c.robot == False
        ).group_by(organization_nodes.c.id)




class OrganizationsProjectCount:
    interface = ProjectCount

    @staticmethod
    def selectable(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.count(projects.c.id).label('project_count')
        ]).select_from(
            organization_nodes.outerjoin(
                projects, projects.c.organization_id == organization_nodes.c.id
            )
        ).group_by(organization_nodes.c.id)


class OrganizationsRepositoryCount:
    interface = RepositoryCount

    @staticmethod
    def selectable(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.count(repositories.c.id).label('repository_count')
        ]).select_from(
            organization_nodes.outerjoin(
                repositories, repositories.c.organization_id == organization_nodes.c.id
            )
        ).group_by(organization_nodes.c.id)
