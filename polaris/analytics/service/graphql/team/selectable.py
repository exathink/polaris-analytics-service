# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene
from datetime import datetime
from sqlalchemy import select, bindparam, func, distinct, true, and_, case, cast, Date, or_, union_all
from polaris.analytics.db.model import teams, contributors_teams, \
    work_item_delivery_cycles, work_items, work_items_teams, \
    work_items_source_state_map, work_item_delivery_cycle_durations, \
    work_items_commits, commits, repositories, contributor_aliases, work_items_sources

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import InterfaceResolver, ConnectionResolver

from ..interfaces import ContributorCount, WorkItemInfo, DeliveryCycleInfo, CycleMetricsTrends, \
    PipelineCycleMetrics, CommitInfo, WorkItemsSourceRef

from ..work_item.sql_expressions import work_item_info_columns, work_item_delivery_cycle_info_columns, \
    work_item_delivery_cycles_connection_apply_filters, CycleMetricsTrendsBase, work_items_connection_apply_filters

from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_filters

from ..utils import date_column_is_in_measurement_window, get_timeline_dates_for_trending
from polaris.analytics.db.enums import WorkItemsStateType

from polaris.utils.exceptions import ProcessingException

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

class TeamWorkItemNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemsSourceRef)

    @classmethod
    def default_connection_nodes_selector(cls, work_items_connection_columns, **kwargs):
        select_stmt = select(
            work_items_connection_columns
        ).select_from(
            teams.join(
                work_items_teams, work_items_teams.c.team_id == teams.c.id
            ).join(
                work_items, work_items_teams.c.work_item_id == work_items.c.id
            ).join(
                work_item_delivery_cycles,
                work_items.c.current_delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
            ).join(
                work_items_sources,
                work_items.c.work_items_source_id == work_items_sources.c.id
            )
        ).where(
            teams.c.key == bindparam('key')
        )
        select_stmt = work_items_connection_apply_filters(select_stmt, work_items, **kwargs)
        return work_item_delivery_cycles_connection_apply_filters(select_stmt, work_items, work_item_delivery_cycles,
                                                                  **kwargs)

    @classmethod
    def connection_nodes_selector(cls, **kwargs):
        work_items_connection_columns = [
            work_items.c.id,
            work_items.c.name,
            work_items.c.key,
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            *work_item_info_columns(work_items),

        ]
        if 'funnel_view' in kwargs:
            return cls.funnel_view_connection_nodes_selector(work_items_connection_columns, **kwargs)
        else:
            return cls.default_connection_nodes_selector(work_items_connection_columns, **kwargs)

    @staticmethod
    def sort_order(project_work_items_nodes, **kwargs):
        return [project_work_items_nodes.c.updated_at.desc()]


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


class TeamCommitNodes(ConnectionResolver):
    interface = CommitInfo

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_team_commits = select([
            *commit_info_columns(repositories, commits, apply_distinct=True),
        ]).select_from(
            teams.join(
                commits, or_(
                    commits.c.author_team_id == teams.c.id,
                    commits.c.committer_team_id == teams.c.id
                )
            ).join(
                repositories, commits.c.repository_id == repositories.c.id
            ).join(
                contributor_aliases, commits.c.committer_contributor_alias_id == contributor_aliases.c.id
            ).outerjoin(
                work_items_commits, work_items_commits.c.commit_id == commits.c.id
            )
        ).where(
            and_(
                teams.c.key == bindparam('key'),
                contributor_aliases.c.robot == False,
                # This filters out specs if nopspecs is given
                work_items_commits.c.commit_id == None if kwargs.get('nospecs_only') else true()
            )
        )
        team_commits = commits_connection_apply_filters(select_team_commits, commits, **kwargs)

        return team_commits


    @staticmethod
    def sort_order(team_commit_nodes, **kwargs):
        return [team_commit_nodes.c.commit_date.desc()]

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


# Team Cycle Metrics Trends

class TeamCycleMetricsTrends(CycleMetricsTrendsBase):
    interface = CycleMetricsTrends

    @staticmethod
    def interface_selector(team_nodes, **kwargs):
        cycle_metrics_trends_args = kwargs.get('cycle_metrics_trends_args')

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            cycle_metrics_trends_args,
            arg_name='cycle_metrics_trends',
            interface_name='CycleMetricTrends'
        )
        measurement_window = cycle_metrics_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating TeamCycleMetricsTrends"
            )

        metrics_map = TeamCycleMetricsTrends.get_metrics_map(
            cycle_metrics_trends_args,
            delivery_cycles_relation=work_item_delivery_cycles
        )
        # Now for each of these dates, we are going to be aggregating the measurements for work items
        # within the measurement window for that date. We will be using a *lateral* join for doing the full aggregation
        # so note the lateral clause at the end instead of the usual alias.
        cycle_metrics = select([
            team_nodes.c.id.label('team_id'),

            # These are standard attributes returned for the the AggregateCycleMetricsInterface

            func.max(work_item_delivery_cycles.c.end_date).label('latest_closed_date'),
            func.min(work_item_delivery_cycles.c.end_date).label('earliest_closed_date'),
            *[
                # This interpolates columns that calculate the specific cycle
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *TeamCycleMetricsTrends.get_metrics_columns(
                    cycle_metrics_trends_args, metrics_map, 'cycle_metrics'
                ),

                # This interpolates columns that calculate the specific work item count related
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *TeamCycleMetricsTrends.get_metrics_columns(
                    cycle_metrics_trends_args, metrics_map, 'work_item_counts'
                ),

                # This interpolates columns that calculate the specific implementation_complexity
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *TeamCycleMetricsTrends.get_metrics_columns(
                    cycle_metrics_trends_args, metrics_map, 'implementation_complexity'
                )
            ]

        ]).select_from(
            team_nodes.join(
                work_items_teams, work_items_teams.c.team_id == team_nodes.c.id
            ).join(
                work_items,
                and_(
                    work_items_teams.c.work_item_id == work_items.c.id,
                    *TeamCycleMetricsTrends.get_work_item_filter_clauses(cycle_metrics_trends_args)
                )
            ).outerjoin(
                # outer join here because we want to report timelines dates even
                # when there are no work items closed in that period.
                work_item_delivery_cycles,
                and_(
                    work_item_delivery_cycles.c.work_item_id == work_items.c.id,
                    # The logic here is as follows:
                    # It measurement date is d, then we will include evey delivery
                    # cycle that closed on the date d which is why the end date is d + 1,
                    # and window-1 days prior. So if window = 1 we will only include the
                    # delivery cycles that closed on the measurement_date.
                    date_column_is_in_measurement_window(
                        work_item_delivery_cycles.c.end_date,
                        measurement_date=timeline_dates.c.measurement_date,
                        measurement_window=measurement_window
                    ),
                    *TeamCycleMetricsTrends.get_work_item_delivery_cycle_filter_clauses(
                        cycle_metrics_trends_args
                    )
                )
            )
        ).group_by(
            team_nodes.c.id
        ).lateral()

        return select([
            cycle_metrics.c.team_id.label('id'),
            func.json_agg(
                func.json_build_object(
                    'measurement_date', timeline_dates.c.measurement_date,
                    'measurement_window', measurement_window,
                    'earliest_closed_date', cycle_metrics.c.earliest_closed_date,
                    'latest_closed_date', cycle_metrics.c.latest_closed_date,
                    'lead_time_target_percentile', cycle_metrics_trends_args.lead_time_target_percentile,
                    'cycle_time_target_percentile', cycle_metrics_trends_args.cycle_time_target_percentile,
                    *[
                        *TeamCycleMetricsTrends.get_cycle_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        ),
                        *TeamCycleMetricsTrends.get_work_item_count_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        ),
                        *TeamCycleMetricsTrends.get_implementation_complexity_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        )
                    ],
                )

            ).label('cycle_metrics_trends')
        ]).select_from(
            timeline_dates.join(cycle_metrics, true())
        ).group_by(
            cycle_metrics.c.team_id
        )


class TeamPipelineCycleMetrics(CycleMetricsTrendsBase):
    interface = PipelineCycleMetrics

    @classmethod
    def get_delivery_cycle_relation_for_pipeline(cls, cycle_metrics_trends_args, measurement_date, team_nodes):
        # This query provides a relation with a column interface similar to
        # work_item_delivery_cycles, but with cycle time and lead time calculated dynamically
        # for work items in the current pipeline. Since cycle_time and lead_time are cached once
        # and for all only on the work items that are closed, we need to calculate these
        # dynamically here. Modulo this, much of the cycle time trending logic is similar across
        # open and closed items, so this lets us share all that logic across both cases.
        return select([
            team_nodes.c.id,
            work_items.c.id.label('work_item_id'),
            # we need these columns to satisfy the delivery_cycle_relation contract for computing metrics
            func.min(work_item_delivery_cycles.c.delivery_cycle_id).label('delivery_cycle_id'),
            func.min(work_item_delivery_cycles.c.end_date).label('end_date'),
            func.min(work_item_delivery_cycles.c.commit_count).label('commit_count'),
            func.min(work_item_delivery_cycles.c.effort).label('effort'),
            func.min(work_item_delivery_cycles.c.earliest_commit).label('earliest_commit'),
            func.max(work_item_delivery_cycles.c.latest_commit).label('latest_commit'),
            # the time from the start of the delivery cycle to the measurement date is the elapsed lead time for this
            # work item
            func.extract('epoch', measurement_date - func.min(work_item_delivery_cycles.c.start_date)).label(
                'lead_time'),

            # cycle time = lead_time - backlog time.
            (
                    func.extract('epoch', measurement_date - func.min(work_item_delivery_cycles.c.start_date)) -
                    # This is calculated backlog time
                    # there can be multiple backlog states, so we could have a duration in each one,
                    # so taking the sum correctly gets you the current backlog duration.
                    func.sum(
                        case(
                            [
                                (work_items_source_state_map.c.state_type == WorkItemsStateType.backlog.value,
                                 work_item_delivery_cycle_durations.c.cumulative_time_in_state)
                            ],
                            else_=0
                        )
                    )
            ).label(
                # using spec_cycle_time here purely to coerce the column name so that
                # we can use the same logic from the base class. The coupling between
                # this method and the base class method needs to be revisited since
                # the logic for the closed and pipeline metrics have now diverged significantly
                # that it may make sense to treat them separately.
                'spec_cycle_time'
            ),
            # Latency for in-progress items = measurement_date - latest_commit
            (
                func.extract(
                    'epoch',
                    measurement_date - func.coalesce(
                        func.min(work_item_delivery_cycles.c.latest_commit),
                        # if latest_commit is null, then its is not a spec - so latency is 0
                        measurement_date
                    )
                )
            ).label('latency')

        ]).select_from(
            # get current delivery cycles for all the work items in the pipeline
            team_nodes.join(
                work_items_teams, work_items_teams.c.team_id == team_nodes.c.id
            ).join(
                work_items,
                work_items_teams.c.work_item_id == work_items.c.id
            ).join(
                work_items_source_state_map,
                work_items_source_state_map.c.work_items_source_id == work_items.c.work_items_source_id
            ).join(
                work_item_delivery_cycles,
                work_item_delivery_cycles.c.delivery_cycle_id == work_items.c.current_delivery_cycle_id
            ).join(
                # along with the durations of all states (could be multiple)
                work_item_delivery_cycle_durations,
                and_(
                    work_item_delivery_cycle_durations.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id,
                    work_item_delivery_cycle_durations.c.state == work_items_source_state_map.c.state
                )
            )
        ).where(
            and_(
                # we include work items that are not in backlog or closed: this the filter for active items only
                work_items.c.state_type.notin_([WorkItemsStateType.backlog.value, WorkItemsStateType.closed.value]),
                # filter out the work items state durations that are in backlog state (could be multiple)
                # This gives the data to calculate the time in backlog so that we can subtract this from
                # lead time to get cycle time
                # work_items_source_state_map.c.state_type == WorkItemsStateType.backlog.value,
                # add any other work item filters that the caller specifies.
                *TeamCycleMetricsTrends.get_work_item_filter_clauses(cycle_metrics_trends_args),
                # add delivery cycle related filters that the caller specifies
                *TeamCycleMetricsTrends.get_work_item_delivery_cycle_filter_clauses(
                    cycle_metrics_trends_args
                )
            )
        ).group_by(
            team_nodes.c.id,
            work_items.c.id,
        ).alias()

    @classmethod
    def interface_selector(cls, team_nodes, **kwargs):
        cycle_metrics_trends_args = kwargs.get('pipeline_cycle_metrics_args')

        measurement_date = datetime.utcnow()

        cycle_times = cls.get_delivery_cycle_relation_for_pipeline(
            cycle_metrics_trends_args, measurement_date, team_nodes)

        metrics_map = TeamCycleMetricsTrends.get_metrics_map(
            cycle_metrics_trends_args,
            delivery_cycles_relation=cycle_times
        )

        # Calculate the cycle metrics
        cycle_metrics = select([
            team_nodes.c.id.label('team_id'),
            *[
                # This interpolates columns that calculate the specific cycle
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *cls.get_metrics_columns(cycle_metrics_trends_args, metrics_map, 'cycle_metrics'),

                # This interpolates columns that calculate the specific work item count
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *cls.get_metrics_columns(cycle_metrics_trends_args, metrics_map, 'work_item_counts'),

                # This interpolates columns that calculate the specific implementation complexity
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *cls.get_metrics_columns(cycle_metrics_trends_args, metrics_map, 'implementation_complexity'),
            ]

        ]).select_from(
            team_nodes.outerjoin(
                cycle_times, team_nodes.c.id == cycle_times.c.id
            )
        ).group_by(
            team_nodes.c.id
        ).alias()

        # Serialize metrics into json columns for returning.
        return select([
            cycle_metrics.c.team_id.label('id'),
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(measurement_date, Date),
                    'lead_time_target_percentile', cycle_metrics_trends_args.lead_time_target_percentile,
                    'cycle_time_target_percentile', cycle_metrics_trends_args.cycle_time_target_percentile,
                    *[
                        *TeamCycleMetricsTrends.get_cycle_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        ),
                        *TeamCycleMetricsTrends.get_work_item_count_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        ),
                        *TeamCycleMetricsTrends.get_implementation_complexity_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        )
                    ],
                )

            ).label('pipeline_cycle_metrics')
        ]).select_from(
            cycle_metrics
        ).group_by(
            cycle_metrics.c.team_id
        )