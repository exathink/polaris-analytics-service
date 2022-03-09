# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.common import db
from sqlalchemy import select, func, bindparam, and_, distinct, extract, between, literal, cast, Date, case
from polaris.graphql.utils import nulls_to_zero, is_paging
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import InterfaceResolver, ConnectionResolver
from polaris.analytics.db.model import repositories, organizations, contributors, commits, \
    repositories_contributor_aliases, contributor_aliases, pull_requests, work_items_commits
from ..interfaces import CommitSummary, ContributorCount, OrganizationRef, CommitInfo, CumulativeCommitCount, \
    CommitCount, WeeklyContributorCount, PullRequestInfo, PullRequestMetricsTrends, TraceabilityTrends

from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_filters
from datetime import datetime, timedelta
from polaris.utils.datetime_utils import time_window
from polaris.utils.exceptions import ProcessingException

from ..utils import date_column_is_in_measurement_window, get_timeline_dates_for_trending

from ..contributor.sql_expressions import contributor_count_apply_contributor_days_filter
from ..pull_request.sql_expressions import pull_request_info_columns, pull_requests_connection_apply_filters


class RepositoryNode:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name
        ]).select_from(
            repositories
        ).where(
            repositories.c.key == bindparam('key')
        )


class RepositoryCommitNodes(ConnectionResolver):
    interface = CommitInfo

    @classmethod
    def connection_nodes_selector(cls, **kwargs):
        select_stmt = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            repositories.join(commits)
        ).where(
            repositories.c.key == bindparam('key')
        )
        return commits_connection_apply_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(repository_commit_nodes, **kwargs):
        return [repository_commit_nodes.c.commit_date.desc()]


class RepositoryPullRequestNodes(ConnectionResolver):
    interfaces = (NamedNode, PullRequestInfo)

    @classmethod
    def connection_nodes_selector(cls, **kwargs):
        select_stmt = select([
            *pull_request_info_columns(pull_requests)
        ]).select_from(
            repositories.join(
                pull_requests,
                pull_requests.c.repository_id == repositories.c.id
            )
        ).where(
            repositories.c.key == bindparam('key')
        )
        return pull_requests_connection_apply_filters(select_stmt, **kwargs)

    @staticmethod
    def sort_order(repository_pull_request_nodes, **kwargs):
        return [repository_pull_request_nodes.c.end_date.desc().nullsfirst()]


class RepositoryContributorNodes(ConnectionResolver):
    interface = NamedNode

    @classmethod
    def connection_nodes_selector(cls, **kwargs):
        return select([
            contributors.c.id,
            contributors.c.key,
            contributors.c.name,
            repositories_contributor_aliases.c.repository_id
        ]).select_from(
            contributors.join(
                    repositories_contributor_aliases.join(
                        repositories
                )
            )
        ).where(
            and_(
                repositories.c.key == bindparam('key'),
                repositories_contributor_aliases.c.robot == False
            )
        ).distinct()


class RepositoryRecentlyActiveContributorNodes(ConnectionResolver):
    interfaces = (NamedNode, CommitCount)

    @classmethod
    def connection_nodes_selector(cls, **kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            commits.c.author_contributor_key.label('key'),
            func.min(commits.c.author_contributor_name).label('name'),
            func.count(commits.c.id).label('commit_count')

        ]).select_from(
            repositories.join(
                commits.join(
                    contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
                )
            )
        ).where(
            and_(
                repositories.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end),
                contributor_aliases.c.robot == False
            )
        ).group_by(
            commits.c.author_contributor_key
        )

    @staticmethod
    def sort_order(recently_active_contributors, **kwargs):
        return [recently_active_contributors.c.commit_count.desc()]


class RepositoryCumulativeCommitCount:

    interface = CumulativeCommitCount

    @staticmethod
    def selectable(**kwargs):
        commit_counts = select([
            extract('year', commits.c.commit_date).label('year'),
            extract('week', commits.c.commit_date).label('week'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            repositories.join(commits)
        ).where(
            repositories.c.key == bindparam('key')
        ).group_by(
            extract('year', commits.c.commit_date),
            extract('week', commits.c.commit_date)
        ).alias()

        return select([
            commit_counts.c.year,
            commit_counts.c.week,
            func.sum(commit_counts.c.commit_count).over(order_by=[
                commit_counts.c.year,
                commit_counts.c.week
            ]).label('cumulative_commit_count')
        ])


class RepositoryWeeklyContributorCount:

    interface = WeeklyContributorCount

    @staticmethod
    def selectable(**kwargs):
        return select([
            extract('year', commits.c.commit_date).label('year'),
            extract('week', commits.c.commit_date).label('week'),
            func.count(distinct(commits.c.author_contributor_key)).label('contributor_count')
        ]).select_from(
            repositories.join(commits)
        ).where(
            repositories.c.key == bindparam('key')
        ).group_by(
            extract('year', commits.c.commit_date),
            extract('week', commits.c.commit_date)
        )


class RepositoriesCommitSummary:
    interface = CommitSummary

    @staticmethod
    def interface_selector(repositories_nodes, **kwargs):
        return select([
            repositories_nodes.c.id,
            repositories.c.commit_count.label('commit_count'),
            repositories.c.earliest_commit.label('earliest_commit'),
            repositories.c.latest_commit.label('latest_commit')

        ]).select_from(
            repositories
        ).where(
            repositories_nodes.c.id == repositories.c.id
        )

    @staticmethod
    def sort_order(repositories_commit_summary, **kwargs):
        return [nulls_to_zero(repositories_commit_summary.c.commit_count).desc()]


class RepositoriesContributorCount:
    interface = ContributorCount

    @staticmethod
    def interface_selector(repositories_nodes, **kwargs):
        select_stmt = select([
            repositories_nodes.c.id,
            func.count(distinct(repositories_contributor_aliases.c.contributor_id)).label('contributor_count')
        ]).select_from(
            repositories_nodes.outerjoin(
                repositories, repositories_nodes.c.id == repositories.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            )
        ).where(
            repositories_contributor_aliases.c.robot == False
        )

        select_stmt = contributor_count_apply_contributor_days_filter(select_stmt, **kwargs)
        return select_stmt.group_by(repositories_nodes.c.id)


class RepositoriesOrganizationRef:
    interface = OrganizationRef

    @staticmethod
    def interface_selector(repositories_nodes, **kwargs):
        return select([
            repositories_nodes.c.id,
            organizations.c.key,
            organizations.c.name.label('organization_name')

        ]).select_from(
            repositories_nodes.outerjoin(
                repositories,
                repositories_nodes.c.id == repositories.c.id
            ).outerjoin(
                organizations, repositories.c.organization_id == organizations.c.id
            )
        )



class RepositoriesPullRequestMetricsTrends(InterfaceResolver):
    interface = PullRequestMetricsTrends

    @staticmethod
    def interface_selector(repositories_nodes, **kwargs):
        pull_request_metrics_trends_args = kwargs.get('pull_request_metrics_trends_args')
        age_target_percentile = pull_request_metrics_trends_args.pull_request_age_target_percentile
        # Get the list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            pull_request_metrics_trends_args,
            arg_name='pull_request_metrics_trends',
            interface_name='PullRequestMetricsTrends'
        )
        repositories_timeline_dates = select([repositories_nodes.c.id, timeline_dates]).cte()

        measurement_window = pull_request_metrics_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectPullRequestMetricsTrends"
            )

        pull_request_attributes = select([
            repositories_timeline_dates.c.id,
            repositories_timeline_dates.c.measurement_date,
            pull_requests.c.id.label('pull_request_id'),
            pull_requests.c.state.label('state'),
            pull_requests.c.end_date,
            (func.extract('epoch', pull_requests.c.end_date - pull_requests.c.created_at) / (1.0 * 3600 * 24)).label(
                'age'),
        ]).select_from(
            repositories_timeline_dates.outerjoin(
                pull_requests, pull_requests.c.repository_id == repositories_timeline_dates.c.id
            )
        ).where(
            and_(
                pull_requests.c.end_date != None,
                date_column_is_in_measurement_window(
                    pull_requests.c.end_date,
                    measurement_date=repositories_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                )
            )
        ).group_by(
            repositories_timeline_dates.c.id,
            repositories_timeline_dates.c.measurement_date,
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
            repositories_timeline_dates.outerjoin(
                pull_request_attributes, and_(
                    repositories_timeline_dates.c.id == pull_request_attributes.c.id,
                    repositories_timeline_dates.c.measurement_date == pull_request_attributes.c.measurement_date
                )
            )).group_by(
            pull_request_attributes.c.measurement_date,
            pull_request_attributes.c.id
        ).order_by(
            pull_request_attributes.c.id,
            pull_request_attributes.c.measurement_date.desc()
        ).alias('pull_request_metrics')

        return select([
            repositories_timeline_dates.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(repositories_timeline_dates.c.measurement_date, Date),
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
            repositories_timeline_dates.outerjoin(
                pull_request_metrics, and_(
                    repositories_timeline_dates.c.id == pull_request_metrics.c.id,
                    repositories_timeline_dates.c.measurement_date == pull_request_metrics.c.measurement_date
                )
            )
        ).group_by(
            repositories_timeline_dates.c.id
        )

class RepositoriesTraceabilityTrends(InterfaceResolver):
    interface = TraceabilityTrends

    @staticmethod
    def interface_selector(repository_nodes, **kwargs):
        traceability_trends_args = kwargs.get('traceability_trends_args')
        measurement_window = traceability_trends_args.measurement_window

        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectCycleMetricsTrends"
            )

        # Get a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            traceability_trends_args,
            arg_name='traceability_trends',
            interface_name='TraceabilityTrends'
        )

        # compute the overall span of dates in the trending window so we can scan and load all the
        # relevant commits that fall within that window. This should be more efficient than scanning
        # all the repositories and then the commits for each measurement point.
        timeline_span = select([
            (func.min(timeline_dates.c.measurement_date) - timedelta(days=measurement_window)).label('window_start'),
            func.max(timeline_dates.c.measurement_date).label('window_end')
        ]).cte()
        # find the candidate commits.
        candidate_commits = select([
            repository_nodes.c.id.label('repository_id'),
            commits.c.id.label('commit_id'),
            commits.c.commit_date
        ]).select_from(
            repository_nodes.join(
                commits, commits.c.repository_id == repository_nodes.c.id
            )
        ).where(
            commits.c.commit_date.between(
                timeline_span.c.window_start,
                timeline_span.c.window_end
            )
        ).cte()

        # do the cross join to compute one row for each repo and each trend measurement date
        # we will compute the traceability metrics for each of these rows
        repositories_timeline_dates = select([repository_nodes, timeline_dates]).alias()

        # we compute the total commits and spec counts for
        # each measurement date and repository combination.
        # for each of these points we are aggrgating the candidate commits
        # that fall within the window to count the commits and specs
        traceability_metrics_base = select([
            repositories_timeline_dates.c.id,
            repositories_timeline_dates.c.measurement_date,
            func.count(candidate_commits.c.commit_id.distinct()).label('total_commits'),
            func.count(candidate_commits.c.commit_id.distinct()).filter(
                work_items_commits.c.work_item_id != None
            ).label('spec_count')
        ]).select_from(
            repositories_timeline_dates.join(
                candidate_commits,
                candidate_commits.c.repository_id == repositories_timeline_dates.c.id
            ).outerjoin(
                work_items_commits,
                work_items_commits.c.commit_id == candidate_commits.c.commit_id
            )
        ).where(
            date_column_is_in_measurement_window(
                candidate_commits.c.commit_date,
                measurement_date=repositories_timeline_dates.c.measurement_date,
                measurement_window=measurement_window
            )
        ).group_by(
            repositories_timeline_dates.c.id,
            repositories_timeline_dates.c.measurement_date
        ).alias()
        # outer join with the timeline dates to make sure we get one entry per repository and date in the
        # series and that the series is ordered by descending date.
        traceability_metrics = select([
            repositories_timeline_dates.c.id,
            repositories_timeline_dates.c.measurement_date,
            traceability_metrics_base.c.total_commits,
            traceability_metrics_base.c.spec_count
        ]).select_from(
            repositories_timeline_dates.outerjoin(
                traceability_metrics_base,
                and_(
                    repositories_timeline_dates.c.id == traceability_metrics_base.c.id,
                    repositories_timeline_dates.c.measurement_date == traceability_metrics_base.c.measurement_date
                )
            )
        ).order_by(
            repositories_timeline_dates.c.id,
            repositories_timeline_dates.c.measurement_date.desc()
        ).alias()
        return select([
            traceability_metrics.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(traceability_metrics.c.measurement_date, Date),
                    'measurement_window', measurement_window,
                    'total_commits', func.coalesce(traceability_metrics.c.total_commits, 0),
                    'spec_count', func.coalesce(traceability_metrics.c.spec_count, 0),
                    'nospec_count', func.coalesce(traceability_metrics.c.total_commits - traceability_metrics.c.spec_count, 0),
                    'traceability', case([
                            (traceability_metrics.c.total_commits > 0, (traceability_metrics.c.spec_count/(traceability_metrics.c.total_commits*1.0)))
                        ], else_=0
                    )
                )
            ).label('traceability_trends')
        ]).select_from(
            traceability_metrics
        ).group_by(
            traceability_metrics.c.id
        )
