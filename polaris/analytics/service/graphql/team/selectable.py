# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene
from sqlalchemy import select, bindparam, func, distinct
from polaris.analytics.db.model import teams, contributors_teams
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import InterfaceResolver

from ..interfaces import ContributorCount


class TeamNode:
    interfaces = (NamedNode,)

    @staticmethod
    def selectable(**kwargs):
        return select([
            teams.c.id,
            teams.c.name,
            teams.c.key,
        ]).where(
            teams.c.key == bindparam('key')
        )


# Interface resolvers

class TeamContributorCount(InterfaceResolver):
    interface = ContributorCount

    @staticmethod
    def interface_selector(team_nodes, **kwargs):
        return select([
            team_nodes.c.id,
            func.count(distinct(contributors_teams.c.contributor_id)).label('contributor_count')
        ]).select_from(
            team_nodes.outerjoin(
                contributors_teams, team_nodes.c.id == contributors_teams.c.team_id
            )
        ).where(
            contributors_teams.c.end_date == None
        ).group_by(
            team_nodes.c.id
        )
