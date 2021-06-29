# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene
from sqlalchemy import select, bindparam, func, distinct
from polaris.analytics.db.model import teams, contributors_teams, \
    work_item_delivery_cycles, work_items, work_items_teams
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import InterfaceResolver, ConnectionResolver

from ..interfaces import ContributorCount, WorkItemInfo, DeliveryCycleInfo
from ..work_item.sql_expressions import work_item_info_columns, work_item_delivery_cycle_info_columns, work_item_delivery_cycles_connection_apply_filters


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


# Connection Resolvers

class TeamWorkItemDeliveryCycleNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, DeliveryCycleInfo)

    def connection_nodes_selector(**kwargs):
        if kwargs.get('active_only'):
            delivery_cycles_join_clause = work_items.c.current_delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
        else:
            delivery_cycles_join_clause = work_item_delivery_cycles.c.work_item_id == work_items.c.id

        select_stmt = select([
            work_item_delivery_cycles.c.delivery_cycle_id.label('id'),
            *work_item_delivery_cycle_info_columns(work_items, work_item_delivery_cycles),
            *work_item_info_columns(work_items),
        ]).select_from(
            teams.join(
                work_items_teams, work_items_teams.c.team_id == teams.c.id
            ).join(
                work_items, work_items_teams.c.work_item_id == work_items.c.id
            ).join(
                work_item_delivery_cycles, delivery_cycles_join_clause
            )
        ).where(
            teams.c.key == bindparam('key')
        )
        return work_item_delivery_cycles_connection_apply_filters(
            select_stmt, work_items, work_item_delivery_cycles, **kwargs
        )

    @staticmethod
    def sort_order(team_work_item_delivery_cycle_nodes, **kwargs):
        return [team_work_item_delivery_cycle_nodes.c.end_date.desc().nullsfirst()]


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
