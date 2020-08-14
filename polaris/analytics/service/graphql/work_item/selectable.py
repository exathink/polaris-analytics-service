# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.common import db

from sqlalchemy import select, bindparam, and_, func, cast, Text, Date

from polaris.analytics.db.model import \
    work_items, work_item_state_transitions, \
    work_items_commits, repositories, commits, \
    work_items_sources, work_item_delivery_cycles, work_items_source_state_map, \
    work_item_delivery_cycle_durations, projects

from polaris.analytics.service.graphql.interfaces import \
    NamedNode, WorkItemInfo, WorkItemCommitInfo, \
    WorkItemsSourceRef, WorkItemStateTransition, CommitInfo, CommitSummary, DeliveryCycleInfo, CycleMetrics, \
    WorkItemStateDetails, WorkItemEventSpan, ProjectRef, ImplementationCost

from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver
from .sql_expressions import work_item_info_columns, work_item_event_columns, work_item_commit_info_columns, \
    work_item_events_connection_apply_time_window_filters, \
    work_item_delivery_cycle_info_columns, work_item_delivery_cycles_connection_apply_filters
from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_time_window_filters, coding_day


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
            (func.min(work_item_delivery_cycles.c.cycle_time) / (1.0 * 3600 * 24)).label('cycle_time'),
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
                work_item_delivery_cycle_durations,
                work_item_delivery_cycle_durations.c.delivery_cycle_id == work_items.c.current_delivery_cycle_id
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


class WorkItemsImplementationCost(InterfaceResolver):
    interface = ImplementationCost

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):

        # Initially we need to compute the load factors for all authors who
        # have committed to this work item by author and coding day, so we can see how many distinct work items
        # each author committed to for each coding day. The inverse of this
        # number is the cost associated with that coding day for that author.
        # So for example if an author committed to 3 work items in a given coding day, then for
        # each of those work items accrues a cost of 1/3 day for that coding day.

        author_coding_days = select([
            commits.c.author_contributor_key,
            coding_day(commits).label('coding_day')
        ]).select_from(
            work_item_nodes.join(
                work_items_commits, work_items_commits.c.work_item_id == work_item_nodes.c.id
            ).join(
                commits, work_items_commits.c.commit_id == commits.c.id
            )
        ).distinct().cte()

        # for each author, coding combo we are computing the number
        # distinct work items that were commited. This is the load factor for that
        # author coding day combo
        author_load_factors = select([
            author_coding_days.c.author_contributor_key,
            author_coding_days.c.coding_day,

            func.count(work_items.c.id.distinct()).label('load_factor')
        ]).select_from(
            # here we are searching over *all* work items (not just the ones in the work_item_nodes)
            # these could be from different projects, work items sources etc. that
            # had commits on the coding days we computed in author_coding_days
            author_coding_days.join(
                commits,
                and_(
                    author_coding_days.c.author_contributor_key == commits.c.author_contributor_key,
                    author_coding_days.c.coding_day == coding_day(commits)
                )
            ).join(
                work_items_commits, work_items_commits.c.commit_id == commits.c.id
            ).join(
                work_items, work_items_commits.c.work_item_id == work_items.c.id
            )
        ).group_by(
            author_coding_days.c.author_contributor_key,
            author_coding_days.c.coding_day
        ).cte()



        # Now we go back and group the work items by work item and author and coding day, joing
        # with the author load factor relation to get the load factor for each author coding day

        work_items_implementation_cost = select([
            work_item_nodes.c.id,
            # note that these are reporting actual commit dates with timestamp, not coding day
            # we need to roll these up at this stage so we dont lose the level of detail
            # when we aggregate at the coding day level, we will do one more level aggregation
            # when we roll up at the work item level below to get the final commit span for the work items
            func.min(commits.c.commit_date).label('earliest_commit'),
            func.max(commits.c.commit_date).label('latest_commit'),
            author_load_factors.c.author_contributor_key,
            author_load_factors.c.coding_day,
            author_load_factors.c.load_factor,
            (1.0/author_load_factors.c.load_factor).label('coding_day_cost')
        ]).select_from(
            work_item_nodes.join(
                work_items_commits, work_items_commits.c.work_item_id == work_item_nodes.c.id
            ).join(
                commits, work_items_commits.c.commit_id == commits.c.id
            ).join(
                author_load_factors,
                and_(
                    author_load_factors.c.author_contributor_key == commits.c.author_contributor_key,
                    author_load_factors.c.coding_day == coding_day(commits)
                )
            )
        ).group_by(
            work_item_nodes.c.id,
            author_load_factors.c.author_contributor_key,
            author_load_factors.c.coding_day,
            author_load_factors.c.load_factor
        ).alias()

        # finally roll up the cost and spans for each work item.

        return select([
            work_items_implementation_cost.c.id,
            # adding up the fractional costs of author coding days for each work item
            # gets us the implementation cost for that work item
            func.sum(work_items_implementation_cost.c.coding_day_cost).label('implementation_cost'),
            # The span of commits dates across all authors coding days gives the implementation span
            (
                func.extract(
                    'epoch',
                    func.max(work_items_implementation_cost.c.latest_commit) -
                    func.min(work_items_implementation_cost.c.earliest_commit)
                )/(1.0*3600*24)
            ).label('implementation_span'),
            func.count(work_items_implementation_cost.c.author_contributor_key.distinct()).label('author_count')
        ]).select_from(
            work_items_implementation_cost
        ).group_by(
            work_items_implementation_cost.c.id
        )