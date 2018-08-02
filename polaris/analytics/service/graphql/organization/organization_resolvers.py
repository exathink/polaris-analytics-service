# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from sqlalchemy import text, select, bindparam, func
from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from polaris.repos.db.model import organizations, repositories
from ..query_utils import select_contributor_summary
from polaris.repos.db.schema import repositories_contributor_aliases, contributor_aliases


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
        ).where(organizations.c.organization_key == bindparam('organization_key'))


class OrganizationCommitSummary:
    interface = CommitSummary

    @staticmethod
    def selectable(organization_node, **kwargs):
        return select([
            organization_node.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')
        ]).select_from(
            organization_node.outerjoin(
                repositories
            )
        ).group_by(organization_node.c.id)


class OrganizationContributorSummary:
    interface = ContributorSummary

    @staticmethod
    def selectable(organization_node, **kwargs):
        contributor_nodes = select([
            organization_node.c.id,
            contributor_aliases.c.id.label('ca_id'),
            contributor_aliases.c.contributor_key
        ]).select_from(
            organization_node.outerjoin(
                repositories
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            ).outerjoin(
                contributor_aliases, contributor_aliases.c.id == repositories_contributor_aliases.c.contributor_alias_id
            )
        ).where(
            contributor_aliases.c.robot == False
        ).cte('contributor_nodes')

        return select_contributor_summary(contributor_nodes)
