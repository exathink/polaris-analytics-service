# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.analytics.service.graphql.interface_mixins import NamedNodeResolverMixin, \
    WorkItemStateMappingResolverMixin, WorkItemStateDetailsResolverMixin, TeamNodeRefsResolverMixin

from polaris.analytics.service.graphql.interfaces import NamedNode, WorkItemInfo, WorkItemStateMapping, \
    WorkItemsSourceRef, WorkItemStateTransition, \
    WorkItemCommitInfo, CommitSummary, DeliveryCycleInfo, WorkItemsStateType, CycleMetrics, \
    WorkItemStateDetails, WorkItemEventSpan, ProjectRef, ImplementationCost, ParentNodeRef, EpicNodeRef, \
    DevelopmentProgress, TeamNodeRefs

from polaris.analytics.service.graphql.work_item.selectable import \
    WorkItemNode, WorkItemEventNodes, WorkItemCommitNodes, WorkItemEventNode, WorkItemCommitNode, \
    WorkItemsCommitSummary, WorkItemDeliveryCycleNode, WorkItemDeliveryCycleNodes, WorkItemDeliveryCycleCycleMetrics, \
    WorkItemsWorkItemStateDetails, WorkItemsWorkItemEventSpan, WorkItemsProjectRef, WorkItemsImplementationCost, \
    WorkItemsParentNodeRef, WorkItemsEpicNodeRef, WorkItemPullRequestNodes, WorkItemPullRequestNode, \
    WorkItemDeliveryCyclesImplementationCost, WorkItemDeliveryCyclesEpicNodeRef, WorkItemsDevelopmentProgress, \
    WorkItemDeliveryCyclesTeamNodeRefs, WorkItemTeamNodeRefs, WorkItemsWorkItemStateMapping \

from polaris.graphql.selectable import ConnectionResolverMixin
from polaris.graphql.selectable import CountableConnection
from polaris.graphql.selectable import Selectable
from ..interface_mixins import KeyIdResolverMixin
from ..commit import CommitsConnectionMixin
from ..pull_request import PullRequestsConnectionMixin, PullRequestNode
from datetime import datetime

from ..arguments import FunnelViewParameters

class WorkItemEvent(
    # interface mixins
    NamedNodeResolverMixin,

    Selectable
):
    class Meta:
        interfaces = (NamedNode, WorkItemInfo, WorkItemStateTransition, WorkItemsSourceRef)
        named_node_resolver = WorkItemEventNode
        interface_resolvers = {}
        connection_class = lambda: WorkItemEvents

    @classmethod
    def key_to_instance_resolver_params(cls, key):
        key_parts = key.split(':')
        assert len(key_parts) == 2
        return dict(work_item_key=key_parts[0], seq_no=key_parts[1])

    @classmethod
    def resolve_field(cls, info, work_item_event_key, **kwargs):
        return cls.resolve_instance(work_item_event_key, **kwargs)


class WorkItemEvents(
    CountableConnection
):
    class Meta:
        node = WorkItemEvent


class WorkItemEventsConnectionMixin(ConnectionResolverMixin):
    work_item_events = WorkItemEvent.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime, required=False,
            description='show events whose eventDate is strictly before this timestamp. '
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return events with eventDate within the specified number of days. "
                        "If before is specified, it returns events with eventDate"
                        "between (before - days) and before"
                        "If before is not specified the it returns events for the"
                        "previous n days starting from utc now"
        )
    )

    def resolve_work_item_events(self, info, **kwargs):
        return WorkItemEvent.resolve_connection(
            self.get_connection_resolver_context('work_item_events'),
            self.get_connection_node_resolver('work_item_events'),
            self.get_instance_query_params(),
            **kwargs
        )


class WorkItemCommit(
    # interface mixins
    KeyIdResolverMixin,
    # selectable
    Selectable
):
    class Meta:
        interfaces = (NamedNode, WorkItemInfo, WorkItemCommitInfo, WorkItemsSourceRef)
        named_node_resolver = WorkItemCommitNode
        interface_resolvers = {}
        connection_class = lambda: WorkItemCommits

    @classmethod
    def key_to_instance_resolver_params(cls, key):
        key_parts = key.split(':')
        assert len(key_parts) == 2
        return dict(work_item_key=key_parts[0], commit_key=key_parts[1])


class WorkItemCommits(
    CountableConnection
):
    class Meta:
        node = WorkItemCommit


class WorkItemCommitsConnectionMixin(ConnectionResolverMixin):
    work_item_commits = WorkItemCommit.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime, required=False,
            description='show commit whose eventDate is strictly before this timestamp. '
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return events with eventDate within the specified number of days. "
                        "If before is specified, it returns events with eventDate"
                        "between (before - days) and before"
                        "If before is not specified the it returns events for the"
                        "previous n days starting from utc now"
        )
    )

    def resolve_work_item_commits(self, info, **kwargs):
        return WorkItemCommit.resolve_connection(
            self.get_connection_resolver_context('work_item_commits'),
            self.get_connection_node_resolver('work_item_commits'),
            self.get_instance_query_params(),
            **kwargs
        )


class WorkItemDeliveryCycle(
    # interface mixins
    NamedNodeResolverMixin,
    TeamNodeRefsResolverMixin,

    Selectable
):
    class Meta:
        interfaces = (NamedNode, WorkItemInfo, DeliveryCycleInfo, CycleMetrics, ImplementationCost, EpicNodeRef, TeamNodeRefs, WorkItemsSourceRef)
        named_node_resolver = WorkItemDeliveryCycleNode
        interface_resolvers = {
            'CycleMetrics': WorkItemDeliveryCycleCycleMetrics,
            'ImplementationCost': WorkItemDeliveryCyclesImplementationCost,
            'EpicNodeRef': WorkItemDeliveryCyclesEpicNodeRef,
            'TeamNodeRefs': WorkItemDeliveryCyclesTeamNodeRefs,
        }
        connection_class = lambda: WorkItemDeliveryCycles

    @classmethod
    def key_to_instance_resolver_params(cls, key):
        key_parts = key.split(':')
        assert len(key_parts) == 2
        return dict(work_item_key=key_parts[0], delivery_cycle_id=key_parts[1])

    @classmethod
    def resolve_field(cls, info, work_item_event_key, **kwargs):
        return cls.resolve_instance(work_item_event_key, **kwargs)


class WorkItemDeliveryCycles(
    CountableConnection
):
    class Meta:
        node = WorkItemDeliveryCycle


class WorkItemDeliveryCyclesConnectionMixin(ConnectionResolverMixin):
    work_item_delivery_cycles = WorkItemDeliveryCycle.ConnectionField(
        closed_before=graphene.Argument(
            graphene.Date, required=False,
            description='Show work_item_delivery_cycles whose end date is before this timestamp'
        ),
        closed_within_days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return work items that were closed within this many days from utc now. This argument is "
                        "required if you are resolve cycle metrics related interfaces in your query"
        ),
        active_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="Return only delivery cycles that are not closed"
        ),
        defects_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="Return only defects"
        ),
        specs_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="Return only delivery cycles with commit_count > 0"
        ),
        state_types=graphene.Argument(
            graphene.List(WorkItemsStateType),
            required=False,
            description="Include only delivery cycles for work items with the specified state types"
        ),
        include_sub_tasks=graphene.Boolean(
            required=False,
            description='Include delivery cycles for work items which are sub-tasks. Defaults to true',
            default_value=True
        ),
        tags=graphene.Argument(
            graphene.List(graphene.String),
            required=False,
            description='Provide a list of tags to filter work_items by',
            default_value=None
        ),
        release=graphene.Argument(
            graphene.String,
            required=False,
            description='Provide a release to filter work_items by',
            default_value=None
        )
    )

    def resolve_work_item_delivery_cycles(self, info, **kwargs):
        return WorkItemDeliveryCycle.resolve_connection(
            self.get_connection_resolver_context('work_item_delivery_cycles'),
            self.get_connection_node_resolver('work_item_delivery_cycles'),
            self.get_instance_query_params(),
            **kwargs
        )


class WorkItem(
    # interface resolver mixins
    NamedNodeResolverMixin,
    WorkItemStateMappingResolverMixin,
    WorkItemStateDetailsResolverMixin,
    TeamNodeRefsResolverMixin,

    # Connection Mixins
    WorkItemEventsConnectionMixin,
    CommitsConnectionMixin,
    WorkItemDeliveryCyclesConnectionMixin,
    PullRequestsConnectionMixin,
    # selectable
    Selectable
):
    class Meta:
        interfaces = (
            NamedNode, WorkItemInfo, WorkItemStateMapping, WorkItemsSourceRef, WorkItemEventSpan, ProjectRef, CommitSummary,
            WorkItemStateDetails, ImplementationCost, ParentNodeRef, EpicNodeRef, DevelopmentProgress,
            TeamNodeRefs
        )
        named_node_resolver = WorkItemNode
        interface_resolvers = {
            'CommitSummary': WorkItemsCommitSummary,
            'WorkItemStateMapping': WorkItemsWorkItemStateMapping,
            'WorkItemStateDetails': WorkItemsWorkItemStateDetails,
            'WorkItemEventSpan': WorkItemsWorkItemEventSpan,
            'ProjectRef': WorkItemsProjectRef,
            'ImplementationCost': WorkItemsImplementationCost,
            'ParentNodeRef': WorkItemsParentNodeRef,
            'EpicNodeRef': WorkItemsEpicNodeRef,
            'DevelopmentProgress': WorkItemsDevelopmentProgress,
            'TeamNodeRefs': WorkItemTeamNodeRefs,
        }
        connection_node_resolvers = {
            'work_item_events': WorkItemEventNodes,
            'work_item_delivery_cycles': WorkItemDeliveryCycleNodes,
            'commits': WorkItemCommitNodes,
            'pull_requests': WorkItemPullRequestNodes
        }
        connection_class = lambda: WorkItems

    @classmethod
    def resolve_field(cls, parent, info, key, **kwargs):
        return cls.resolve_instance(key=key, **kwargs)


class WorkItems(
    CountableConnection
):
    class Meta:
        node = WorkItem


class WorkItemsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    work_items = WorkItem.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime, required=False,
            description='show work_items whose latest update is strictly before this timestamp. '
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return work items last updated within the specified number of days. "
                        "If before is specified, it returns work items updated "
                        "between (before - days) and before"
                        "If before is not specified then it returns work items for the"
                        "previous n days starting from utc now"
        ),
        active_within_days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return work items that were active in the last n days",
        ),
        closed_within_days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return work items that were closed in the last n days"
        ),
        active_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="Return only delivery cycles that are in open, wip, or complete phases"
        ),
        funnel_view=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="Return a record for the each work item that is currently in "
                        "backlog, open, wip, or complete state (the top of the funnel) "
                        "and a record for each closed delivery cycle "
                        "for closed work items (bottom of the funnel)."
                        "A work item may show up once in the top of the funnel and multiple times"
                        "in the bottom of the funnel if it has many delivery cycles. Funnel view can be combined"
                        "with specs_only, defects_only and closed_within_days params to limit the selection further."
        ),
        funnel_view_args=graphene.Argument(
            FunnelViewParameters,
            required=False,
            description="If funnel view is true specify arguments for how the funnel view should handled"
                        "closed and non-closed states"
        ),
        specs_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="Return only delivery cycles with commit_count > 0"
        ),
        defects_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="Return only defects"
        ),
        state_types=graphene.Argument(
            graphene.List(WorkItemsStateType),
            required=False,
            description="Include only work items with the specified state types"
        ),
        include_epics=graphene.Boolean(
            required=False,
            description='Include epics in the work items. Defaults to false',
            default_value=False
        ),
        include_sub_tasks=graphene.Boolean(
            required=False,
            description='Include subtasks in the work items. Defaults to true',
            default_value=True
        ),
        suppress_moved_items=graphene.Boolean(
            required=False,
            description='Exclude items which have been moved from current source',
            default_value=True
        ),
        tags=graphene.Argument(
            graphene.List(graphene.String),
            required=False,
            description='Provide a list of tags to filter work_items by',
            default_value=None
        ),
        release=graphene.Argument(
            graphene.String,
            required=False,
            description='Provide a  release name to filter work_items by',
            default_value=None
        )
    )

    def resolve_work_items(self, info, **kwargs):
        return WorkItem.resolve_connection(
            self.get_connection_resolver_context('work_items'),
            self.get_connection_node_resolver('work_items'),
            self.get_instance_query_params(),
            **kwargs
        )


class RecentlyActiveWorkItemsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    recently_active_work_items = WorkItem.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime,
            required=False,
            description="End date of period to search for commit activity. If not specified it defaults to utc now"
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            default_value=7,
            description="Return work items with commits within the specified number of days"
        )
    )

    def resolve_recently_active_work_items(self, info, **kwargs):
        return WorkItem.resolve_connection(
            self.get_connection_resolver_context('recently_active_work_items'),
            self.get_connection_node_resolver('recently_active_work_items'),
            self.get_instance_query_params(),
            **kwargs
        )


class WorkItemPullRequest(
    # interface mixins
    NamedNodeResolverMixin,

    Selectable
):
    class Meta:
        interfaces = (NamedNode,)
        named_node_resolver = WorkItemPullRequestNode
        interface_resolvers = {}

        connection_class = lambda: WorkItemPullRequests

    @classmethod
    def key_to_instance_resolver_params(cls, key):
        key_parts = key.split(':')
        assert len(key_parts) == 2
        return dict(work_item_key=key_parts[0], pull_request_id=key_parts[1])

    @classmethod
    def resolve_field(cls, info, work_item_event_key, **kwargs):
        return cls.resolve_instance(work_item_event_key, **kwargs)


class WorkItemPullRequests(
    CountableConnection
):
    class Meta:
        node = WorkItemPullRequest
