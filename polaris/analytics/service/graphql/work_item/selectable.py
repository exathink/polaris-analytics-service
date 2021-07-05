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
    pull_requests, work_items_pull_requests, work_items_teams, teams

from polaris.analytics.service.graphql.interfaces import \
    NamedNode, WorkItemInfo, WorkItemCommitInfo, \
    WorkItemsSourceRef, WorkItemStateTransition, CommitInfo, CommitSummary, DeliveryCycleInfo, CycleMetrics, \
    WorkItemStateDetails, WorkItemEventSpan, ProjectRef, ImplementationCost, ParentNodeRef, EpicNodeRef, \
    PullRequestInfo, \
    DevelopmentProgress, \
    TeamNodeRefs

from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver
from .sql_expressions import work_item_info_columns, work_item_event_columns, work_item_commit_info_columns, \
    work_item_events_connection_apply_time_window_filters, \
    work_item_delivery_cycle_info_columns, work_item_delivery_cycles_connection_apply_filters
from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_filters, commit_day
from ..pull_request.sql_expressions import pull_request_info_columns


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


class WorkItemsDevelopmentProgress(InterfaceResolver):
    interface = DevelopmentProgress

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        non_epic_work_items = select([
            work_item_nodes.c.id
        ]).select_from(
            work_item_nodes.join(
                work_items, work_item_nodes.c.id == work_items.c.id
            )
        ).where(
            work_items.c.is_epic == False
        ).cte()

        non_epic_work_items_span = select([
            non_epic_work_items.c.id,
            (case([
                (func.sum(
                    case([
                        (work_item_delivery_cycles.c.end_date == None, 1)
                    ], else_=0)) > 0, False)
            ], else_=True)).label('closed'),
            func.min(work_item_delivery_cycles.c.start_date).label('start_date'),
            (case([
                (func.sum(case([
                    (work_item_delivery_cycles.c.end_date == None, 1)
                ], else_=0)) > 0, None)
            ], else_=func.max(work_item_delivery_cycles.c.end_date))).label('end_date'),
            func.max(work_item_delivery_cycles.c.latest_commit).label('last_update')
        ]).select_from(
            non_epic_work_items.outerjoin(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == non_epic_work_items.c.id
            )
        ).group_by(
            non_epic_work_items.c.id
        ).alias()

        non_epic_work_items_progress = select([
            non_epic_work_items.c.id,
            non_epic_work_items_span.c.closed,
            non_epic_work_items_span.c.start_date,
            non_epic_work_items_span.c.end_date,
            non_epic_work_items_span.c.last_update,
            (case([
                (non_epic_work_items_span.c.end_date == None,
                 func.extract('epoch', datetime.now() - non_epic_work_items_span.c.start_date) / (1.0 * 3600 * 24))
            ], else_=func.extract('epoch',
                                  non_epic_work_items_span.c.end_date - non_epic_work_items_span.c.start_date) / (
                             1.0 * 3600 * 24))).label('elapsed')
        ]).select_from(
            non_epic_work_items.outerjoin(
                non_epic_work_items_span, non_epic_work_items_span.c.id == non_epic_work_items.c.id
            )
        ).group_by(
            non_epic_work_items.c.id,
            non_epic_work_items_span.c.closed,
            non_epic_work_items_span.c.start_date,
            non_epic_work_items_span.c.end_date,
            non_epic_work_items_span.c.last_update
        )

        epics = select([
            work_item_nodes.c.id
        ]).select_from(
            work_item_nodes.join(
                work_items, work_item_nodes.c.id == work_items.c.id
            )
        ).where(
            work_items.c.is_epic == True
        ).cte()

        epics_span = select([
            epics.c.id,
            (case([
                (func.sum(
                    case([
                        (work_item_delivery_cycles.c.end_date == None, 1)
                    ], else_=0)) > 0, False)
            ], else_=True)).label('closed'),
            func.min(work_item_delivery_cycles.c.start_date).label('start_date'),
            (case([
                (func.sum(case([
                    (work_item_delivery_cycles.c.end_date == None, 1)
                ], else_=0)) > 0, None)
            ], else_=func.max(work_item_delivery_cycles.c.end_date))).label('end_date'),
            func.max(work_item_delivery_cycles.c.latest_commit).label('last_update')
        ]).select_from(
            epics.outerjoin(
                work_items, work_items.c.parent_id == epics.c.id
            ).outerjoin(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
            )
        ).group_by(
            epics.c.id
        ).alias()

        epics_progress = select([
            epics.c.id,
            epics_span.c.closed,
            epics_span.c.start_date,
            epics_span.c.end_date,
            epics_span.c.last_update,
            (case([
                (epics_span.c.end_date == None,
                 func.extract('epoch', datetime.now() - epics_span.c.start_date) / (1.0 * 3600 * 24))
            ], else_=func.extract('epoch', epics_span.c.end_date - epics_span.c.start_date) / (
                    1.0 * 3600 * 24))).label('elapsed')

        ]).select_from(
            epics.outerjoin(
                epics_span, epics_span.c.id == epics.c.id
            )
        ).group_by(
            epics.c.id,
            epics_span.c.closed,
            epics_span.c.start_date,
            epics_span.c.end_date,
            epics_span.c.last_update
        )

        return epics_progress.union(non_epic_work_items_progress)


class WorkItemDeliveryCycleCycleMetrics(InterfaceResolver):
    interface = CycleMetrics

    @staticmethod
    def interface_selector(work_item_delivery_cycle_nodes, **kwargs):
        return select([
            work_item_delivery_cycle_nodes.c.id,
            (func.min(work_item_delivery_cycles.c.lead_time) / (1.0 * 3600 * 24)).label('lead_time'),
            func.min(work_item_delivery_cycles.c.end_date).label('end_date'),
            (func.min(work_item_delivery_cycles.c.spec_cycle_time) / (1.0 * 3600 * 24)).label('cycle_time'),
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


class WorkItemDeliveryCyclesImplementationCost(InterfaceResolver):
    interface = ImplementationCost

    @staticmethod
    def interface_selector(work_item_delivery_cycle_nodes, **kwargs):
        return select([
            work_item_delivery_cycle_nodes.c.id,
            func.min(work_item_delivery_cycles.c.effort).label('effort'),
            (case([
                (func.min(work_item_delivery_cycles.c.commit_count) > 0,
                 func.min(
                     func.extract('epoch',
                                  work_item_delivery_cycles.c.latest_commit - work_item_delivery_cycles.c.earliest_commit) / (
                             1.0 * 3600 * 24)
                 ))
            ], else_=None)).label('duration'),
            func.count(contributor_aliases.c.contributor_id.distinct()).label('author_count')
        ]).select_from(
            work_item_delivery_cycle_nodes.outerjoin(
                work_item_delivery_cycles,
                work_item_delivery_cycle_nodes.c.id == work_item_delivery_cycles.c.delivery_cycle_id
            ).outerjoin(
                work_item_delivery_cycle_contributors,
                work_item_delivery_cycle_contributors.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id,
            ).join(
                contributor_aliases,
                work_item_delivery_cycle_contributors.c.contributor_alias_id == contributor_aliases.c.id
            )).group_by(
            work_item_delivery_cycle_nodes.c.id
        )


class WorkItemDeliveryCyclesEpicNodeRef(InterfaceResolver):
    interface = EpicNodeRef

    @staticmethod
    def interface_selector(work_item_delivery_cycle_nodes, **kwargs):
        parents = work_items.alias()
        return select([
            work_item_delivery_cycle_nodes.c.id,
            func.min(parents.c.name).label('epic_name'),
            func.min(cast(parents.c.key, Text)).label('epic_key')
        ]).select_from(
            work_item_delivery_cycle_nodes.join(
                parents, and_(
                    work_item_delivery_cycle_nodes.c.parent_id == parents.c.id,
                    parents.c.is_epic == True
                )
            )
        ).group_by(
            work_item_delivery_cycle_nodes.c.id
        )


class WorkItemDeliveryCyclesTeamNodeRefs(InterfaceResolver):
    interface = TeamNodeRefs

    @staticmethod
    def interface_selector(work_item_delivery_cycle_nodes, **kwargs):
        return select([
            work_item_delivery_cycle_nodes.c.id,
            func.json_agg(
                func.json_build_object(
                    'team_name', teams.c.name,
                    'team_key', teams.c.key
                )
            ).label('team_node_refs')
        ]).select_from(
            work_item_delivery_cycle_nodes.outerjoin(
                work_items_teams, work_item_delivery_cycle_nodes.c.work_item_id == work_items_teams.c.work_item_id
            ).outerjoin(
                teams, work_items_teams.c.team_id == teams.c.id
            )
        ).group_by(
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
                ),
                'delivery_cycle_info',
                func.json_build_object(
                    'closed',
                    func.bool_and(work_item_delivery_cycles.c.end_date != None),
                    'start_date',
                    func.min(work_item_delivery_cycles.c.start_date),
                    'end_date',
                    func.min(work_item_delivery_cycles.c.end_date)
                ),
                'cycle_metrics',
                func.json_build_object(
                    'lead_time',
                    func.min(work_item_delivery_cycles.c.lead_time)/(24 * 3600 * 1.0),
                    'cycle_time',
                    func.min(work_item_delivery_cycles.c.spec_cycle_time)/(24 * 3600 * 1.0)
                ),
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
        non_epics_implementation_cost = select([
            work_item_nodes.c.id,
            func.max(work_items.c.budget).label('budget'),
            func.max(work_items.c.effort).label('effort'),
            (func.extract(
                'epoch',
                func.max(work_item_delivery_cycles.c.latest_commit) -
                func.min(work_item_delivery_cycles.c.earliest_commit)
            ) / (1.0 * 24 * 3600)).label('duration'),

            func.count(work_item_delivery_cycle_contributors.c.contributor_alias_id.distinct()).label('author_count')
        ]).select_from(
            work_item_nodes.join(
                work_items, work_item_nodes.c.id == work_items.c.id
            ).outerjoin(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
            ).outerjoin(
                work_item_delivery_cycle_contributors,
                work_item_delivery_cycle_contributors.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
            )
        ).group_by(
            work_item_nodes.c.id
        ).where(work_items.c.is_epic == False)

        epics = select([
            work_item_nodes.c.id,
            work_items.c.budget,
        ]).select_from(
            work_item_nodes.join(
                work_items, work_item_nodes.c.id == work_items.c.id
            )
        ).where(
            work_items.c.is_epic == True
        ).cte()

        epic_cost_details = select([
            epics.c.id.label('epic_id'),
            func.sum(work_items.c.effort).label('effort')
        ]).select_from(
            epics.outerjoin(
                work_items, work_items.c.parent_id == epics.c.id
            )
        ).group_by(
            epics.c.id
        ).alias()

        epic_commits = select([
            epics.c.id.label('epic_id'),
            (func.extract(
                'epoch',
                func.max(work_item_delivery_cycles.c.latest_commit) -
                func.min(work_item_delivery_cycles.c.earliest_commit)
            ) / (1.0 * 24 * 3600)).label('duration'),
            func.count(work_item_delivery_cycle_contributors.c.contributor_alias_id.distinct()).label('author_count')
        ]).select_from(
            epics.join(
                work_items, work_items.c.parent_id == epics.c.id
            ).outerjoin(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
            ).outerjoin(
                work_item_delivery_cycle_contributors,
                work_item_delivery_cycle_contributors.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
            )
        ).group_by(
            epics.c.id
        ).alias()

        epics_implementation_cost = select([
            epics.c.id,
            epics.c.budget,
            epic_cost_details.c.effort,
            epic_commits.c.duration,
            epic_commits.c.author_count
        ]).select_from(
            epics.outerjoin(
                epic_cost_details, epic_cost_details.c.epic_id == epics.c.id
            ).outerjoin(
                epic_commits, epic_commits.c.epic_id == epics.c.id
            )
        ).group_by(
            epics.c.id,
            epics.c.budget,
            epic_cost_details.c.effort,
            epic_commits.c.duration,
            epic_commits.c.author_count
        )
        return non_epics_implementation_cost.union(epics_implementation_cost)


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
            *pull_request_info_columns(pull_requests),

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


class WorkItemTeamNodeRefs(InterfaceResolver):
    interface = TeamNodeRefs

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        return select([
            work_item_nodes.c.id,
            func.json_agg(
                func.json_build_object(
                    'team_name', teams.c.name,
                    'team_key', teams.c.key
                )
            ).label('team_node_refs')
        ]).select_from(
            work_item_nodes.outerjoin(
                work_items_teams, work_item_nodes.c.id == work_items_teams.c.work_item_id
            ).outerjoin(
                teams, work_items_teams.c.team_id == teams.c.id
            )
        ).group_by(
            work_item_nodes.c.id
        )