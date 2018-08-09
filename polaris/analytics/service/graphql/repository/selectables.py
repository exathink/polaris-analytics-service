# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func, bindparam

from polaris.graphql.interfaces import NamedNode
from ..interfaces import CommitSummary, ContributorSummary, OrganizationRef
from ..selectables import select_contributor_summary

from polaris.repos.db.model import repositories, organizations
from polaris.repos.db.schema import contributor_aliases, repositories_contributor_aliases

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


class RepositoriesContributorSummary:
    interface = ContributorSummary

    @staticmethod
    def selectable(repositories_nodes, **kwargs):
        contributor_nodes = select([
            repositories_nodes.c.id,
            contributor_aliases.c.id.label('ca_id'),
            contributor_aliases.c.contributor_key
        ]).select_from(
            repositories_nodes.outerjoin(
                repositories, repositories_nodes.c.id == repositories.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            ).outerjoin(
                contributor_aliases, contributor_aliases.c.id == repositories_contributor_aliases.c.contributor_alias_id
            )
        ).where(
            contributor_aliases.c.robot == False
        ).cte('contributor_nodes')

        return select_contributor_summary(contributor_nodes)

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
