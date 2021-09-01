# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene
from datetime import datetime, timedelta
from sqlalchemy import select, bindparam, func, distinct, true, and_, case, cast, Date, or_, union_all, literal
from polaris.analytics.db.model import teams, contributors_teams, \
    work_item_delivery_cycles, work_items, work_items_teams, \
    work_items_source_state_map, work_item_delivery_cycle_durations, \
    work_items_commits, commits, repositories, contributor_aliases, work_items_sources, pull_requests, \
    teams_repositories

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import InterfaceResolver, ConnectionResolver

from ..interfaces import ContributorCount, WorkItemInfo, DeliveryCycleInfo, CycleMetricsTrends, \
    PipelineCycleMetrics, CommitInfo, WorkItemsSourceRef, PullRequestInfo, CommitSummary, FlowMixTrends, \
    PullRequestMetricsTrends, CapacityTrends, TeamInfo

from ..work_item.sql_expressions import work_item_info_columns, work_item_delivery_cycle_info_columns, \
    work_item_delivery_cycles_connection_apply_filters, CycleMetricsTrendsBase, work_items_connection_apply_filters, \
    map_work_item_type_to_flow_type

from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_filters, commit_day

from ..utils import date_column_is_in_measurement_window, get_timeline_dates_for_trending

from ..pull_request.sql_expressions import pull_request_info_columns

from polaris.analytics.db.enums import WorkItemsStateType

from polaris.utils.exceptions import ProcessingException


class TeamNode:
    interfaces = (NamedNode, TeamInfo)

    @staticmethod
    def selectable(**kwargs):
        return select([
            teams.c.id,
            teams.c.name,
            teams.c.key,
            teams.c.settings
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
                repositories, and_(
                    commits.c.repository_id == repositories.c.id,
                    repositories.c.organization_id == teams.c.organization_id
                )
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


class TeamPullRequestMetricsTrends(InterfaceResolver):
    interface = PullRequestMetricsTrends

    @staticmethod
    def interface_selector(team_nodes, **kwargs):
        pull_request_metrics_trends_args = kwargs.get('pull_request_metrics_trends_args')
        age_target_percentile = pull_request_metrics_trends_args.pull_request_age_target_percentile
        # Get the list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            pull_request_metrics_trends_args,
            arg_name='pull_request_metrics_trends',
            interface_name='PullRequestMetricsTrends'
        )
        team_timeline_dates = select([team_nodes.c.id, timeline_dates]).cte()

        measurement_window = pull_request_metrics_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectPullRequestMetricsTrends"
            )

        pull_request_attributes = select([
            team_timeline_dates.c.id,
            team_timeline_dates.c.measurement_date,
            pull_requests.c.id.label('pull_request_id'),
            pull_requests.c.state.label('state'),
            pull_requests.c.updated_at,
            (func.extract('epoch', pull_requests.c.updated_at - pull_requests.c.created_at) / (1.0 * 3600 * 24)).label(
                'age'),
        ]).select_from(
            team_timeline_dates.outerjoin(
                teams_repositories, team_timeline_dates.c.id == teams_repositories.c.team_id
            ).join(
                repositories, repositories.c.id == teams_repositories.c.repository_id
            ).join(
                pull_requests, pull_requests.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                pull_requests.c.state != 'open',
                date_column_is_in_measurement_window(
                    pull_requests.c.updated_at,
                    measurement_date=team_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                )
            )
        ).group_by(
            team_timeline_dates.c.id,
            team_timeline_dates.c.measurement_date,
            pull_requests.c.id
        ).alias('pull_request_attributes')

        pull_request_metrics = select([
            pull_request_attributes.c.id,
            pull_request_attributes.c.measurement_date,
            func.avg(pull_request_attributes.c.age).label('avg_age'),
            func.min(pull_request_attributes.c.age).label('min_age'),
            func.max(pull_request_attributes.c.age).label('max_age'),
            func.percentile_disc(age_target_percentile).within_group(pull_request_attributes.c.age).label(
                'percentile_age'),
            func.count(pull_request_attributes.c.pull_request_id).label('total_closed'),
            literal(0).label('total_open')
        ]).select_from(
            team_timeline_dates.outerjoin(
                pull_request_attributes, and_(
                    team_timeline_dates.c.id == pull_request_attributes.c.id,
                    team_timeline_dates.c.measurement_date == pull_request_attributes.c.measurement_date
                )
            )).group_by(
            pull_request_attributes.c.measurement_date,
            pull_request_attributes.c.id
        ).order_by(
            pull_request_attributes.c.id,
            pull_request_attributes.c.measurement_date.desc()
        ).alias('pull_request_metrics')

        return select([
            team_timeline_dates.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(team_timeline_dates.c.measurement_date, Date),
                    'measurement_window', measurement_window,
                    'total_open', func.coalesce(pull_request_metrics.c.total_open, 0),
                    'total_closed', func.coalesce(pull_request_metrics.c.total_closed, 0),
                    'avg_age', func.coalesce(pull_request_metrics.c.avg_age, 0),
                    'min_age', func.coalesce(pull_request_metrics.c.min_age, 0),
                    'max_age', func.coalesce(pull_request_metrics.c.max_age, 0),
                    'percentile_age', func.coalesce(pull_request_metrics.c.percentile_age, 0)
                )
            ).label('pull_request_metrics_trends')
        ]).select_from(
            team_timeline_dates.outerjoin(
                pull_request_metrics, and_(
                    team_timeline_dates.c.id == pull_request_metrics.c.id,
                    team_timeline_dates.c.measurement_date == pull_request_metrics.c.measurement_date
                )
            )
        ).group_by(
            team_timeline_dates.c.id
        )

class TeamPullRequestNodes(ConnectionResolver):
    interfaces = (NamedNode, PullRequestInfo)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_pull_requests = select([
            *pull_request_info_columns(pull_requests)
        ]).distinct().select_from(
            teams.join(
                commits,
                or_(
                    commits.c.author_team_id == teams.c.id,
                    commits.c.committer_team_id == teams.c.id
                )
            ).join(
                repositories,
                and_(
                    commits.c.repository_id == repositories.c.id,
                    repositories.c.organization_id == teams.c.organization_id
                )
            ).join(
                pull_requests, pull_requests.c.repository_id == repositories.c.id
            )
        ).where(
            teams.c.key == bindparam('key')
        )

        if kwargs.get('active_only'):
            select_pull_requests = select_pull_requests.where(pull_requests.c.state == 'open')

        if 'closed_within_days' in kwargs:
            window_start = datetime.utcnow() - timedelta(days=kwargs.get('closed_within_days'))

            select_pull_requests = select_pull_requests.where(
                and_(
                    pull_requests.c.state != 'open',
                    pull_requests.c.end_date >= window_start
                )
            )

        return select_pull_requests

    @staticmethod
    def sort_order(pull_request_nodes, **kwargs):
        return [pull_request_nodes.c.created_at.desc().nullsfirst()]


# Interface resolvers

class TeamCommitSummary(InterfaceResolver):
    interface = CommitSummary

    @staticmethod
    def interface_selector(team_nodes, **kwargs):
        return select([
            team_nodes.c.id,
            func.min(teams_repositories.c.earliest_commit).label('earliest_commit'),
            func.max(teams_repositories.c.latest_commit).label('latest_commit'),
            func.sum(teams_repositories.c.commit_count).label('commit_count')

        ]).select_from(
            team_nodes.join(
                teams, team_nodes.c.id == teams.c.id
            ).outerjoin(
                teams_repositories, teams_repositories.c.team_id == teams.c.id
            ).outerjoin(
                repositories, teams_repositories.c.repository_id == repositories.c.id
            )
        ).where(
            # we need to limit this to the current organization because teams may be associated with cross org repos
            # which will give odd results if shown within an org.
            repositories.c.organization_id == teams.c.organization_id
        ).group_by(
            team_nodes.c.id
        )


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


class TeamFlowMixTrends(InterfaceResolver):
    interface = FlowMixTrends

    @staticmethod
    def interface_selector(team_nodes, **kwargs):
        flow_mix_trends_args = kwargs.get('flow_mix_trends_args')

        measurement_window = flow_mix_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating FlowMixTrends"
            )

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            flow_mix_trends_args,
            arg_name='flow_mix_trends',
            interface_name='FlowMixTrends'
        )

        teams_timeline_dates = select([team_nodes.c.id, timeline_dates]).cte()

        select_work_items = select([
            teams_timeline_dates.c.id,
            teams_timeline_dates.c.measurement_date,
            work_items.c.id.label('work_item_id'),
            work_items.c.work_item_type,
            map_work_item_type_to_flow_type(work_items).label('category'),
            work_item_delivery_cycles.c.effort.label('effort')
        ]).select_from(
            teams_timeline_dates.join(
                work_items_teams, work_items_teams.c.team_id == teams_timeline_dates.c.id
            ).join(
                work_items, work_items_teams.c.work_item_id == work_items.c.id
            ).join(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
            )
        ).where(
            and_(
                date_column_is_in_measurement_window(
                    work_item_delivery_cycles.c.end_date,
                    measurement_date=teams_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                ),
                *TeamCycleMetricsTrends.get_work_item_filter_clauses(flow_mix_trends_args),
                *TeamCycleMetricsTrends.get_work_item_delivery_cycle_filter_clauses(
                    flow_mix_trends_args
                )
            )
        ).cte()

        select_category_counts = select([
            teams_timeline_dates.c.id,
            teams_timeline_dates.c.measurement_date,
            select_work_items.c.category,
            func.count(select_work_items.c.work_item_id.distinct()).label('work_item_count'),
            func.sum(select_work_items.c.effort).label('total_effort')
        ]).select_from(
            teams_timeline_dates.outerjoin(
                select_work_items,
                and_(
                    teams_timeline_dates.c.id == select_work_items.c.id,
                    teams_timeline_dates.c.measurement_date == select_work_items.c.measurement_date
                )
            )
        ).group_by(
            teams_timeline_dates.c.id,
            teams_timeline_dates.c.measurement_date,
            select_work_items.c.category
        ).alias()

        select_flow_mix = select([
            select_category_counts.c.id,
            select_category_counts.c.measurement_date,
            func.json_agg(
                func.json_build_object(
                    'category',
                    select_category_counts.c.category,
                    'work_item_count',
                    select_category_counts.c.work_item_count,
                    'total_effort',
                    select_category_counts.c.total_effort,
                )
            ).label('flow_mix')
        ]).select_from(
            select_category_counts
        ).group_by(
            select_category_counts.c.id,
            select_category_counts.c.measurement_date
        ).order_by(
            select_category_counts.c.id,
            select_category_counts.c.measurement_date.desc(),
        ).alias()

        return select([
            select_flow_mix.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', select_flow_mix.c.measurement_date,
                    'measurement_window', measurement_window,
                    'flow_mix', select_flow_mix.c.flow_mix
                )
            ).label('flow_mix_trends')
        ]).select_from(
            select_flow_mix
        ).group_by(
            select_flow_mix.c.id
        )


class TeamCapacityTrends(InterfaceResolver):
    interface = CapacityTrends

    @classmethod
    def get_aggregate_capacity_trends(cls, measurement_window, team_nodes, timeline_dates):

        select_capacity = select([
            team_nodes.c.id,
            timeline_dates.c.measurement_date,
            commits.c.author_contributor_key,
            func.count(commit_day(commits).distinct()).label('commit_days')
        ]).select_from(
            timeline_dates.join(
                team_nodes, true()
            ).join(
                commits, or_(
                    commits.c.author_team_id == team_nodes.c.id,
                    commits.c.committer_team_id == team_nodes.c.id
                )
            ).join(
                contributor_aliases, commits.c.committer_contributor_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                date_column_is_in_measurement_window(
                    commits.c.commit_date,
                    measurement_date=timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                ),
                contributor_aliases.c.robot == False,
            )
        ).group_by(
            team_nodes.c.id,
            timeline_dates.c.measurement_date,
            commits.c.author_contributor_key
        ).alias()

        capacity_metrics = select([
            select_capacity.c.id,
            select_capacity.c.measurement_date,
            func.sum(select_capacity.c.commit_days).label('total_commit_days'),
            func.avg(select_capacity.c.commit_days).label('avg_commit_days'),
            func.min(select_capacity.c.commit_days).label('min_commit_days'),
            func.max(select_capacity.c.commit_days).label('max_commit_days'),
            func.count(select_capacity.c.author_contributor_key.distinct()).label('contributor_count')
        ]).select_from(
            select_capacity
        ).group_by(
            select_capacity.c.id,
            select_capacity.c.measurement_date
        ).order_by(
            select_capacity.c.id,
            select_capacity.c.measurement_date.desc()
        ).alias()
        return select([
            capacity_metrics.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', capacity_metrics.c.measurement_date,
                    'measurement_window', measurement_window,
                    'total_commit_days', capacity_metrics.c.total_commit_days,
                    'avg_commit_days', capacity_metrics.c.avg_commit_days,
                    'min_commit_days', capacity_metrics.c.min_commit_days,
                    'max_commit_days', capacity_metrics.c.max_commit_days,
                    'contributor_count', capacity_metrics.c.contributor_count
                )
            ).label('capacity_trends')
        ]).select_from(
            capacity_metrics,
        ).group_by(
            capacity_metrics.c.id
        )

    @classmethod
    def get_contributor_level_capacity_trends(cls, measurement_window, team_nodes, timeline_dates):

        # Note: we are doing a very different query strategy here compared to
        # other trending interfaces. Rather than joining against projects_timeline_dates,
        # derived from joining project_nodes to timeline_dates, we are doing a cross join of the projects table
        # with timelinedates and using the commit date filter to select commits, and *then* limiting by the
        # id's in the project_nodes. We are doing this because
        # the regular approach was leading to query plans in prod where it was always doing a table scan on
        # the commits table. For some reason, the  query planner was ignoring the indexes on commit_date and repository
        # id and selecting the commits in scope using a full table scan of commits, which is obviously slow.
        # the current strategy is about 500x faster as result. Dont have a clear explanation as to why
        # the other plan was so bad, but going with this approach here since it is functionally equivalent, even if a
        # bit less obvious in a logical sense.

        select_capacity = select([
            team_nodes.c.id,
            timeline_dates.c.measurement_date,
            commits.c.author_contributor_key.label('contributor_key'),
            commits.c.author_contributor_name.label('contributor_name'),
            commit_day(commits).label('commit_day')
        ]).select_from(
            timeline_dates.join(
                team_nodes, true()
            ).join(
                commits, or_(
                    commits.c.author_team_id == team_nodes.c.id,
                    commits.c.committer_team_id == team_nodes.c.id
                )
            ).join(
                contributor_aliases, commits.c.committer_contributor_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                date_column_is_in_measurement_window(
                    commits.c.commit_date,
                    measurement_date=timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                ),
                contributor_aliases.c.robot == False,
            )
        ).alias()

        capacity_metrics = select([
            select_capacity.c.id,
            select_capacity.c.measurement_date,
            select_capacity.c.contributor_key,
            select_capacity.c.contributor_name,
            func.count(select_capacity.c.commit_day.distinct()).label('total_commit_days'),
        ]).select_from(
            select_capacity
        ).group_by(
            select_capacity.c.id,
            select_capacity.c.measurement_date,
            select_capacity.c.contributor_key,
            select_capacity.c.contributor_name,
        ).order_by(
            select_capacity.c.id,
            select_capacity.c.measurement_date.desc()
        ).alias()
        return select([
            capacity_metrics.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', capacity_metrics.c.measurement_date,
                    'measurement_window', measurement_window,
                    'contributor_key', capacity_metrics.c.contributor_key,
                    'contributor_name', capacity_metrics.c.contributor_name,
                    'total_commit_days', capacity_metrics.c.total_commit_days,
                )
            ).label('contributor_detail')
        ]).select_from(
            capacity_metrics,
        ).group_by(
            capacity_metrics.c.id
        )

    @classmethod
    def interface_selector(cls, team_nodes, **kwargs):
        capacity_trends_args = kwargs.get('capacity_trends_args')

        measurement_window = capacity_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating CapacityTrends"
            )

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            capacity_trends_args,
            arg_name='capacity_trends',
            interface_name='CapacityTrends'
        )

        if capacity_trends_args.show_contributor_detail:
            capacity_trends = cls.get_aggregate_capacity_trends(measurement_window, team_nodes,
                                                                timeline_dates).alias()
            contributor_detail = cls.get_contributor_level_capacity_trends(measurement_window,
                                                                           team_nodes, timeline_dates).alias()

            return select([
                team_nodes.c.id,
                capacity_trends.c.capacity_trends,
                contributor_detail.c.contributor_detail,
            ]).select_from(
                team_nodes.outerjoin(
                    capacity_trends, capacity_trends.c.id == team_nodes.c.id
                ).outerjoin(
                    contributor_detail, contributor_detail.c.id == team_nodes.c.id
                )
            )
        else:
            return cls.get_aggregate_capacity_trends(measurement_window, team_nodes, timeline_dates)