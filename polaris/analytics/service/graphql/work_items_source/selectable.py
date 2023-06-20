# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from polaris.analytics.db.enums import WorkItemsStateType

from sqlalchemy import select, bindparam, func, case, union_all, literal_column, and_, Text, literal

from polaris.analytics.db.model import work_items_sources, work_items, work_item_state_transitions, repositories, \
    commits, work_items_commits, work_items_source_state_map
from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemsSourceRef, WorkItemStateTransition, \
    WorkItemCommitInfo
from polaris.graphql.base_classes import NamedNodeResolver, ConnectionResolver, InterfaceResolver
from polaris.graphql.interfaces import NamedNode
from ..commit.sql_expressions import commits_connection_apply_filters
from ..interfaces import WorkItemStateMappings
from ..work_item.sql_expressions import work_item_info_columns, work_items_connection_apply_filters, \
    work_item_event_columns, work_item_events_connection_apply_time_window_filters, work_item_commit_info_columns


class WorkItemsSourceNode(NamedNodeResolver):
    interfaces = (NamedNode,)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            work_items_sources.c.id,
            work_items_sources.c.key.label('key'),
            work_items_sources.c.name,

        ]).select_from(
            work_items_sources
        ).where(
            work_items_sources.c.key == bindparam('key')
        )


class WorkItemsSourceWorkItemNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemsSourceRef)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            work_items.c.name,
            work_items.c.key,
            *work_item_info_columns(work_items)
        ]).select_from(
            work_items.join(
                work_items_sources, work_items_sources.c.id == work_items.c.work_items_source_id
            )
        ).where(
            work_items_sources.c.key == bindparam('key')
        )
        return work_items_connection_apply_filters(select_stmt, work_items, **kwargs)


class WorkItemsSourceWorkItemEventNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemStateTransition, WorkItemsSourceRef)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            *work_item_event_columns(work_items, work_item_state_transitions)
        ]).select_from(
            work_items_sources.join(
                work_items, work_items_sources.c.id == work_items.c.work_items_source_id
            ).join(
                work_item_state_transitions
            )
        ).where(
            work_items_sources.c.key == bindparam('key')
        )
        return work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions, **kwargs)

    @staticmethod
    def sort_order(project_work_item_event_nodes, **kwargs):
        return [project_work_item_event_nodes.c.event_date.desc()]


class WorkItemsSourceWorkItemCommitNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemCommitInfo, WorkItemsSourceRef)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            *work_item_info_columns(work_items),
            *work_item_commit_info_columns(work_items, repositories, commits)
        ]).select_from(
            work_items_sources.join(
                work_items, work_items_sources.c.id == work_items.c.work_items_source_id
            ).join(
                work_items_commits
            ).join(
                commits
            ).join(
                repositories
            )
        ).where(
            work_items_sources.c.key == bindparam('key')
        )
        return commits_connection_apply_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(project_work_item_commits_nodes, **kwargs):
        return [project_work_item_commits_nodes.c.commit_date.desc()]


class WorkItemsSourceWorkItemStateMappings(InterfaceResolver):
    interface = WorkItemStateMappings

    @staticmethod
    def interface_selector(work_items_source_nodes, **kwargs):
        current_state_mapping = select([
            work_items_source_nodes.c.id,
            work_items_source_state_map.c.state,
            work_items_source_state_map.c.state_type,
            work_items_source_state_map.c.flow_type,
            work_items_source_state_map.c.release_status
        ]).distinct().select_from(
            work_items_source_nodes.outerjoin(
                work_items_source_state_map,
                work_items_source_nodes.c.id == work_items_source_state_map.c.work_items_source_id
            ),
        )

        unmapped_states = select([
            work_items_source_nodes.c.id,
            work_items.c.state,
            literal_column(f"'{WorkItemsStateType.unmapped.value}'").label('state_type'),
            literal(None).label('flow_type'),
            literal(None).label('release_status')
        ]).distinct().select_from(
            work_items_source_nodes.outerjoin(
                work_items, work_items.c.work_items_source_id == work_items_source_nodes.c.id
            ).outerjoin(
                work_items_source_state_map,
                and_(
                    work_items_source_state_map.c.work_items_source_id == work_items_source_nodes.c.id,
                    work_items.c.state == work_items_source_state_map.c.state
                )
            )
        ).where(
            work_items_source_state_map.c.state == None
        )

        state_mapping = union_all(
            current_state_mapping,
            unmapped_states
        ).alias()

        return select([
            state_mapping.c.id,

            func.json_agg(
                case([
                    (
                        and_(state_mapping.c.id != None, state_mapping.c.state != None),
                        func.json_build_object(
                            'state', state_mapping.c.state,
                            'state_type', state_mapping.c.state_type,
                            'flow_type', state_mapping.c.flow_type,
                            'release_status', state_mapping.c.release_status
                        )
                    )
                ], else_=None)
        ).label('work_item_state_mappings')

        ]).select_from(
            state_mapping
        ).group_by(
            state_mapping.c.id
        )
