# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, and_, func, case
from datetime import datetime, timedelta

from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver
from polaris.utils.exceptions import ProcessingException
from polaris.analytics.db.model import \
    work_items, work_item_state_transitions, \
    work_items_commits, repositories, commits, \
    work_items_sources, work_item_delivery_cycles, work_items_source_state_map,\
    work_item_delivery_cycle_durations

from polaris.analytics.db.enums import WorkItemsStateType

from polaris.analytics.service.graphql.interfaces import \
    NamedNode, WorkItemInfo, WorkItemCommitInfo, \
    WorkItemsSourceRef, WorkItemStateTransition, CommitInfo, CommitSummary, DeliveryCycleInfo, CycleMetrics

from .sql_expressions import work_item_info_columns, work_item_event_columns, work_item_commit_info_columns, \
    work_item_events_connection_apply_time_window_filters, work_item_cycle_time_column_expr,\
    work_item_delivery_cycle_info_columns, work_item_delivery_cycles_connection_apply_filters

from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_time_window_filters


class WorkItemNode(NamedNodeResolver):
    interfaces = (NamedNode, WorkItemInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            work_items.c.id,
            work_items.c.key,
            work_items.c.name,
            *work_item_info_columns(work_items)
        ]).where(
            work_items.c.key == bindparam('key')
        )


# ------------------------------------------------
# WorkItemEvents connection for work items
# ------------------------------------------------

# a single work item event accessed via its node id of the form work_item_key:seq_no
class WorkItemEventNode(NamedNodeResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemStateTransition, WorkItemsSourceRef)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            *work_item_event_columns(work_items, work_item_state_transitions),
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),

        ]).select_from(
            work_items.join(
                work_item_state_transitions, work_item_state_transitions.c.work_item_id == work_items.c.id
            ).join(
                work_items_sources, work_items_sources.c.id == work_items.c.work_items_source_id
            )
        ).where(
            and_(
                work_items.c.key == bindparam('work_item_key'),
                work_item_state_transitions.c.seq_no == bindparam('seq_no')
            )
        )


class WorkItemEventNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemsSourceRef, WorkItemInfo, WorkItemStateTransition)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            *work_item_event_columns(work_items, work_item_state_transitions)
        ]).where(
            and_(
                work_items_sources.c.id == work_items.c.work_items_source_id,
                work_item_state_transitions.c.work_item_id == work_items.c.id,
                work_items.c.key == bindparam('key')
            )
        )

        return work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions, **kwargs)


# --------------------------------------------------
# WorkItemCommits Connection for work items
# ---------------------------------------------------

# a single work_item_commit accessed by its node id of the form work_item_key:commit_key
class WorkItemCommitNode(NamedNodeResolver):
    interfaces = (WorkItemInfo, WorkItemCommitInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            *work_item_info_columns(work_items),
            *work_item_commit_info_columns(work_items, repositories, commits)
        ]).select_from(
            work_items.join(
                work_items_commits
            ).join(
                commits
            ).join(
                repositories
            )
        ).where(
            and_(
                work_items.c.key == bindparam('work_item_key'),
                commits.c.key == bindparam('commit_key')
            )
        )


class WorkItemCommitNodes(ConnectionResolver):
    interface = CommitInfo

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            work_items.join(
                work_items_commits
            ).join(
                commits
            ).join(
                repositories
            )
        ).where(
            work_items.c.key == bindparam('key')
        )
        return commits_connection_apply_time_window_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(work_item_commit_nodes, **kwargs):
        return [work_item_commit_nodes.c.commit_date.desc()]


# --------------------------------------------------
# WorkItemDeliveryCycle Connection for work items
# ---------------------------------------------------

# a single work_item_delivery_cycle  accessed by its node id of the form work_item_key:commit_key

class WorkItemDeliveryCycleNode(NamedNodeResolver):
    interfaces = (NamedNode, WorkItemInfo, DeliveryCycleInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            work_item_delivery_cycles.c.delivery_cycle_id.label('id'),
            *work_item_info_columns(work_items),
            *work_item_delivery_cycle_info_columns(work_items, work_item_delivery_cycles)
        ]).select_from(
            work_items.join(
                work_item_delivery_cycles
            )
        ).where(
            and_(
                work_items.c.key == bindparam('work_item_key'),
                work_item_delivery_cycles.c.delivery_cycle_id == bindparam('delivery_cycle_id')
            )
        )


class WorkItemDeliveryCycleNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, DeliveryCycleInfo)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_item_delivery_cycles.c.delivery_cycle_id.label('id'),
            *work_item_info_columns(work_items),
            *work_item_delivery_cycle_info_columns(work_items, work_item_delivery_cycles)
        ]).select_from(
            work_items.join(
                work_item_delivery_cycles
            )
        ).where(
            work_items.c.key == bindparam('key')
        )
        return work_item_delivery_cycles_connection_apply_filters(select_stmt, work_items, work_item_delivery_cycles,
                                                                  **kwargs)

    @staticmethod
    def sort_order(work_item_delivery_cycle_nodes, **kwargs):
        return [work_item_delivery_cycle_nodes.c.end_date.desc().nullsfirst()]


class WorkItemDeliveryCycleCycleMetrics(InterfaceResolver):
    interface = CycleMetrics

    @staticmethod
    def interface_selector(work_item_delivery_cycle_nodes, **kwargs):
        return select([
            work_item_delivery_cycle_nodes.c.id,
            (func.min(work_item_delivery_cycles.c.lead_time) / (1.0 * 3600 * 24)).label('lead_time'),
            # We return cycle time only for closed items.
            case([
                (func.min(work_item_delivery_cycles.c.end_date) != None, work_item_cycle_time_column_expr())
            ], else_=None).label('cycle_time'),

            func.min(work_item_delivery_cycles.c.end_date).label('end_date'),
        ]).select_from(
            work_item_delivery_cycle_nodes.outerjoin(
                work_item_delivery_cycles,
                work_item_delivery_cycle_nodes.c.id == work_item_delivery_cycles.c.delivery_cycle_id
            ).outerjoin(
                work_item_delivery_cycle_durations,
                work_item_delivery_cycle_durations.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
            ).join(
                work_items_source_state_map,
                and_(
                    work_item_delivery_cycle_nodes.c.work_items_source_id == work_items_source_state_map.c.work_items_source_id,
                    work_item_delivery_cycle_durations.c.state == work_items_source_state_map.c.state
                )
            )).group_by(
            work_item_delivery_cycle_nodes.c.id
        )


# -----------------------------
# Work Item Interface Resolvers
# -----------------------------

class WorkItemsCommitSummary(InterfaceResolver):
    interface = CommitSummary

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        return select([
            work_item_nodes.c.id,
            func.count(commits.c.commit_date).label('commit_count'),
            func.min(commits.c.commit_date).label('earliest_commit'),
            func.max(commits.c.commit_date).label('latest_commit')

        ]).select_from(
            work_item_nodes.join(
                work_items_commits, work_items_commits.c.work_item_id == work_item_nodes.c.id
            ).join(
                commits, commits.c.id == work_items_commits.c.commit_id
            )
        ).group_by(work_item_nodes.c.id)
