# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func, bindparam

from ..interfaces import CommitSummary, ContributorSummary, NamedNode
from ..selectables import select_contributor_summary

from polaris.repos.db.model import organizations, projects, repositories
from polaris.repos.db.schema import repositories, contributor_aliases, repositories_contributor_aliases

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

class OrganizationsContributorSummary:
    interface = ContributorSummary

    @staticmethod
    def selectable(organization_nodes, **kwargs):

        contributor_nodes = select([
            organization_nodes.c.id,
            contributor_aliases.c.id.label('ca_id'),
            contributor_aliases.c.contributor_key
        ]).select_from(
            organization_nodes.outerjoin(
                repositories, repositories.c.organization_id == organization_nodes.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            ).outerjoin(
                contributor_aliases, contributor_aliases.c.id == repositories_contributor_aliases.c.contributor_alias_id
            )
        ).where (
            contributor_aliases.c.robot == False
        ).cte('contributor_nodes')


        return select_contributor_summary(contributor_nodes)


