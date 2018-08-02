# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import *
from .interfaces import CommitSummary

def select_unique_contributors(contributor_nodes):
    return select([
        contributor_nodes.c.id,
        func.count(distinct(contributor_nodes.c.contributor_key)).label('unique_contributor_count'),
    ]).select_from(
        contributor_nodes
    ).group_by(contributor_nodes.c.id)


def select_unassigned_aliases(contributor_nodes):
    return select([
        contributor_nodes.c.id,
        func.count(distinct(contributor_nodes.c.ca_id)).label('unassigned_alias_count')
    ]).select_from(
        contributor_nodes
    ).where(contributor_nodes.c.contributor_key == None). \
        group_by(contributor_nodes.c.id)


def select_contributor_summary(contributor_nodes):
    unique_contributors = select_unique_contributors(contributor_nodes).cte('unique_contributors')

    unassigned_aliases = select_unassigned_aliases(contributor_nodes).cte('unassigned_aliases')

    return select([
        unique_contributors.c.id,
        case([
            (unassigned_aliases.c.unassigned_alias_count == None, 0),
        ], else_=unassigned_aliases.c.unassigned_alias_count).label('unassigned_alias_count'),
        unique_contributors.c.unique_contributor_count,
        (
                case([
                    (unassigned_aliases.c.unassigned_alias_count == None, 0),
                ], else_=unassigned_aliases.c.unassigned_alias_count)
                + unique_contributors.c.unique_contributor_count
        ).label('contributor_count')
    ]).select_from(
        unique_contributors.outerjoin(unassigned_aliases, unique_contributors.c.id == unassigned_aliases.c.id)
    )


class ProjectsCommitSummariesSelectable:
    interface = CommitSummary

    @staticmethod
    def selectable(organization_projects_nodes, **kwargs):
        return select([
            organization_projects_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            organization_projects_nodes.outerjoin(
                projects_repositories, organization_projects_nodes.c.id == projects_repositories.c.project_id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            )
        ).group_by(organization_projects_nodes.c.id)