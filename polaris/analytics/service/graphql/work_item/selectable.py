# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.common import db

from sqlalchemy import select, bindparam, and_, func, cast, Text, Date, case, literal
from datetime import datetime

from polaris.analytics.db.model import \
    work_items, work_item_state_transitions, \
    work_items_commits, repositories, commits, \
    work_items_sources, work_item_delivery_cycles, work_items_source_state_map, \
    work_item_delivery_cycle_durations, projects, \
    work_item_delivery_cycle_contributors, contributor_aliases, contributors, \
    pull_requests, work_items_pull_requests

from polaris.analytics.service.graphql.interfaces import \
    NamedNode, WorkItemInfo, WorkItemCommitInfo, \
    WorkItemsSourceRef, WorkItemStateTransition, CommitInfo, CommitSummary, DeliveryCycleInfo, CycleMetrics, \
    WorkItemStateDetails, WorkItemEventSpan, ProjectRef, ImplementationCost, ParentNodeRef, EpicNodeRef, PullRequestInfo

from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver
from .sql_expressions import work_item_info_columns, work_item_event_columns, work_item_commit_info_columns, \
    work_item_events_connection_apply_time_window_filters, \
    work_item_delivery_cycle_info_columns, work_item_delivery_cycles_connection_apply_filters
from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_filters, commit_day


class WorkItemNode(NamedNodeResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemsSourceRef)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            work_items.c.id,
            work_items.c.key,
            work_items.c.name,
            *work_item_info_columns(work_items),
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type')
        ]).select_from(
            work_items.join(
                work_items_sources, work_items.c.work_items_source_id == work_items_sources.c.id
            )
        ).where(
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
        previous_state_type = work_items_source_state_map.alias()
        new_state_type = work_items_source_state_map.alias()
        return select([
            *work_item_event_columns(work_items, work_item_state_transitions),
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            previous_state_type.c.state_type.label('previous_state_type'),
            new_state_type.c.state_type.label('new_state_type')

        ]).select_from(
            work_items.join(
                work_item_state_transitions, work_item_state_transitions.c.work_item_id == work_items.c.id
            ).join(
                work_items_sources, work_items_sources.c.id == work_items.c.work_items_source_id
            ).outerjoin(
                previous_state_type, and_(
                    previous_state_type.c.work_items_source_id == work_items_sources.c.id,
                    work_item_state_transitions.c.previous_state == previous_state_type.c.state
                )
            ).outerjoin(
                new_state_type, and_(
                    new_state_type.c.work_items_source_id == work_items_sources.c.id,
                    work_item_state_transitions.c.state == new_state_type.c.state
                )
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
        previous_state_type = work_items_source_state_map.alias()
        new_state_type = work_items_source_state_map.alias()
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            *work_item_event_columns(work_items, work_item_state_transitions),
            previous_state_type.c.state_type.label('previous_state_type'),
            new_state_type.c.state_type.label('new_state_type')
        ]).select_from(
            work_items.join(
                work_item_state_transitions, work_item_state_transitions.c.work_item_id == work_items.c.id
            ).join(
                work_items_sources, work_items_sources.c.id == work_items.c.work_items_source_id
            ).outerjoin(
                previous_state_type, and_(
                    previous_state_type.c.work_items_source_id == work_items_sources.c.id,
                    work_item_state_transitions.c.previous_state == previous_state_type.c.state
                )
            ).outerjoin(
                new_state_type, and_(
                    new_state_type.c.work_items_source_id == work_items_sources.c.id,
                    work_item_state_transitions.c.state == new_state_type.c.state
                )
            )
        ).where(
            work_items.c.key == bindparam('key')
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
        return commits_connection_apply_filters(select_stmt, commits, **kwargs)

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
            func.min(work_item_delivery_cycles.c.end_date).label('end_date'),
            (func.min(work_item_delivery_cycles.c.cycle_time) / (1.0 * 3600 * 24)).label('cycle_time'),
            (case([
                (func.min(work_item_delivery_cycles.c.commit_count) > 0,
                 func.min(
                     func.extract('epoch',
                                  work_item_delivery_cycles.c.latest_commit - work_item_delivery_cycles.c.earliest_commit) / (
                             1.0 * 3600 * 24)
                 ))
            ], else_=None)).label('duration'),
            (func.min(work_item_delivery_cycles.c.latency) / (1.0 * 3600 * 24)).label('latency'),
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

class WorkItemsWorkItemStateDetails(InterfaceResolver):
    interface = WorkItemStateDetails

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        return select([
            work_item_nodes.c.id,
            func.json_build_object(
                'current_state_transition',
                func.json_build_object(
                    'event_date', func.min(work_item_state_transitions.c.created_at),
                    'seq_no', func.min(work_item_state_transitions.c.seq_no),
                    'previous_state', func.min(work_item_state_transitions.c.previous_state),
                    'new_state', func.min(work_item_state_transitions.c.state)
                ),
                'current_delivery_cycle_durations',
                func.json_agg(
                    func.json_build_object(
                        'state', work_item_delivery_cycle_durations.c.state,
                        'state_type', work_items_source_state_map.c.state_type,
                        'days_in_state',
                        work_item_delivery_cycle_durations.c.cumulative_time_in_state / (1.0 * 3600 * 24)
                    )
                ),
                'commit_summary',
                func.json_build_object(
                    'commit_count',
                    func.min(work_item_delivery_cycles.c.commit_count),
                    'earliest_commit',
                    func.min(work_item_delivery_cycles.c.earliest_commit),
                    'latest_commit',
                    func.min(work_item_delivery_cycles.c.latest_commit)
                ),
                'implementation_cost',
                func.json_build_object(
                    'effort',
                    func.min(work_item_delivery_cycles.c.effort),
                    'duration',
                    (
                            func.extract(
                                'epoch',
                                func.min(work_item_delivery_cycles.c.latest_commit) -
                                func.min(work_item_delivery_cycles.c.earliest_commit)
                            ) / (24 * 3600 * 1.0)
                    ).label('duration')

                )
            ).label('work_item_state_details')

        ]).select_from(
            work_item_nodes.outerjoin(
                work_items, work_items.c.id == work_item_nodes.c.id
            ).outerjoin(
                work_item_state_transitions,
                and_(
                    work_item_state_transitions.c.work_item_id == work_items.c.id,
                    work_item_state_transitions.c.seq_no == work_items.c.next_state_seq_no - 1
                )
            ).outerjoin(
                work_item_delivery_cycles,
                work_item_delivery_cycles.c.delivery_cycle_id == work_items.c.current_delivery_cycle_id
            ).outerjoin(
                work_item_delivery_cycle_durations,
                work_item_delivery_cycle_durations.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
            ).outerjoin(
                work_items_source_state_map,
                and_(
                    work_item_delivery_cycle_durations.c.state == work_items_source_state_map.c.state,
                    work_items_source_state_map.c.work_items_source_id == work_items.c.work_items_source_id
                )
            )
        ).group_by(work_item_nodes.c.id)


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


class WorkItemsWorkItemEventSpan(InterfaceResolver):
    interface = WorkItemEventSpan

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        return select([
            work_item_nodes.c.id,
            func.min(work_item_state_transitions.c.created_at).label('earliest_work_item_event'),
            func.max(work_item_state_transitions.c.created_at).label('latest_work_item_event')

        ]).select_from(
            work_item_nodes.join(
                work_item_state_transitions, work_item_state_transitions.c.work_item_id == work_item_nodes.c.id
            )
        ).group_by(work_item_nodes.c.id)


class WorkItemsProjectRef(InterfaceResolver):
    interface = ProjectRef

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        return select([
            work_item_nodes.c.id,
            func.min(cast(projects.c.key, Text)).label('project_key'),
            func.min(projects.c.name).label('project_name')
        ]).select_from(
            work_item_nodes.join(
                work_items_sources, work_item_nodes.c.work_items_source_id == work_items_sources.c.id
            ).join(
                projects, work_items_sources.c.project_id == projects.c.id
            )
        ).group_by(work_item_nodes.c.id)


# ------------------------------
# Generic parent node and epic node ref implementations.


class WorkItemsParentNodeRef(InterfaceResolver):
    interface = ParentNodeRef

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        parents = work_items.alias()
        return select([
            work_item_nodes.c.id,
            func.min(parents.c.name).label('parent_name'),
            func.min(cast(parents.c.key, Text)).label('parent_key')
        ]).select_from(
            work_item_nodes.join(
                work_items, work_items.c.id == work_item_nodes.c.id
            ).outerjoin(
                parents, work_items.c.parent_id == parents.c.id
            )

        ).group_by(work_item_nodes.c.id)


class WorkItemsEpicNodeRef(InterfaceResolver):
    interface = EpicNodeRef

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        parents = work_items.alias()
        return select([
            work_item_nodes.c.id,
            func.min(parents.c.name).label('epic_name'),
            func.min(cast(parents.c.key, Text)).label('epic_key')
        ]).select_from(
            work_item_nodes.join(
                work_items, work_items.c.id == work_item_nodes.c.id
            ).outerjoin(
                parents, and_(
                    work_items.c.parent_id == parents.c.id,
                    parents.c.is_epic == True
                )
            )
        ).group_by(work_item_nodes.c.id)


class WorkItemsImplementationCost(InterfaceResolver):
    interface = ImplementationCost

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        return select([
            work_item_nodes.c.id,
            func.max(work_items.c.effort).label('effort'),
            (func.extract(
                'epoch',
                func.max(commits.c.commit_date).label('latest_commit') -
                func.min(commits.c.commit_date).label('earliest_commit')
            ) / (1.0 * 24 * 3600)).label('duration'),

            func.count(commits.c.author_contributor_key.distinct()).label('author_count')
        ]).select_from(
            work_item_nodes.join(
                work_items, work_item_nodes.c.id == work_items.c.id
            ).join(
                work_items_commits, work_items_commits.c.work_item_id == work_items.c.id
            ).join(
                commits, work_items_commits.c.commit_id == commits.c.id
            )
        ).group_by(
            work_item_nodes.c.id
        )


class WorkItemPullRequestNode(NamedNodeResolver):
    interfaces = (NamedNode,)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            work_items_pull_requests.c.pull_request_id.label('id'),
        ]).select_from(
            work_items.join(
                work_items_pull_requests
            )
        ).where(
            and_(
                work_items.c.key == bindparam('work_item_key'),
                work_items_pull_requests.c.pull_request_id == bindparam('pull_request_id')
            )
        )


class WorkItemPullRequestNodes(ConnectionResolver):
    interfaces = (NamedNode, PullRequestInfo)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            pull_requests.c.id.label('id'),
            pull_requests.c.key.label('key'),
            pull_requests.c.title.label('name'),
            pull_requests.c.created_at.label('created_at'),
            pull_requests.c.state.label('state'),
            pull_requests.c.merged_at.label('merged_at'),
            (func.extract('epoch',
                          case(
                              [
                                  (pull_requests.c.state != 'open',
                                   (pull_requests.c.updated_at - pull_requests.c.created_at))
                              ],
                              else_=(datetime.utcnow() - pull_requests.c.created_at)
                          )

                          ) / (1.0 * 3600 * 24)).label('age')
        ]).select_from(
            work_items.join(
                work_items_pull_requests, work_items_pull_requests.c.work_item_id == work_items.c.id
            ).join(
                pull_requests, pull_requests.c.id == work_items_pull_requests.c.pull_request_id
            )
        ).where(
            work_items.c.key == bindparam('key')
        )

    @staticmethod
    def sort_order(pull_request_nodes, **kwargs):
        return [pull_request_nodes.c.created_at.desc().nullsfirst()]
