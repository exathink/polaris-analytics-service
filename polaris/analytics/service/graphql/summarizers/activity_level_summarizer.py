# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.connection_utils import ConnectionSummarizer
from polaris.graphql.exceptions import InvalidSummarizerException
from polaris.graphql.utils import days_between
from sqlalchemy import Column, DateTime
from polaris.common import db
from sqlalchemy import select, literal_column, func, extract, between, union_all
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
from ..interfaces import ActivityLevelSummary

SECS_IN_DAY = 60 * 60 * 24  # secs


class ActivityLevelSummarizer(ConnectionSummarizer):
    class Meta:
        interface = ActivityLevelSummary

    @classmethod
    def summarize_db(cls, connection_query_temp, session):
        if 'latest_commit' in connection_query_temp.c:
            now = datetime.utcnow()

            active_count = select([
                literal_column("'active_count'").label('category'),
                func.count(connection_query_temp.c.key).label('count')
            ]).where(
                func.trunc((extract('epoch', now) - extract('epoch',
                                                            connection_query_temp.c.latest_commit)) / SECS_IN_DAY) <= 30
            )
            quiescent_count = select([
                literal_column("'quiescent_count'").label('category'),
                func.count(connection_query_temp.c.key).label('count')
            ]).where(
                between(func.trunc(
                    (extract('epoch', now) - extract('epoch', connection_query_temp.c.latest_commit)) / SECS_IN_DAY),
                        31, 90)
            )

            dormant_count = select([
                literal_column("'dormant_count'").label('category'),
                func.count(connection_query_temp.c.key).label('count')
            ]).where(
                between(func.trunc(
                    (extract('epoch', now) - extract('epoch', connection_query_temp.c.latest_commit)) / SECS_IN_DAY),
                    91,
                    180)
            )

            inactive_count = select([
                literal_column("'inactive_count'").label('category'),
                func.count(connection_query_temp.c.key).label('count')
            ]).where(
                func.trunc((extract('epoch', now) - extract('epoch',
                                                            connection_query_temp.c.latest_commit)) / SECS_IN_DAY) > 180
            )

            summary_query = union_all(active_count, quiescent_count, dormant_count, inactive_count).alias(
                'activity_level_summary')

            result = session.connection.execute(select([summary_query.c.category, summary_query.c.count])).fetchall()

            return {row['category']: row['count'] for row in result}

        else:
            raise InvalidSummarizerException(
                f'Class {cls.__name__} cannot summarize temp table {connection_query_temp}. Missing Column: latest_commit'
            )

    @classmethod
    def summarize_result_set(cls, result_set):
        activity_level_summary = dict(active_count=0, quiescent_count=0, dormant_count=0, inactive_count=0)
        now = datetime.utcnow()
        for row in result_set:
            latest_commit = getattr(row, 'latest_commit', None)
            if latest_commit:
                days_since_latest_commit = days_between(latest_commit, now)
                if days_since_latest_commit <= 30:
                    activity_level_summary['active_count'] = activity_level_summary['active_count'] + 1
                elif 30 < days_since_latest_commit <= 90:
                    activity_level_summary['quiescent_count'] = activity_level_summary['quiescent_count'] + 1
                elif 90 < days_since_latest_commit <= 180:
                    activity_level_summary['dormant_count'] = activity_level_summary['dormant_count'] + 1
                else:
                    activity_level_summary['inactive_count'] = activity_level_summary['inactive_count'] + 1
            else:
                raise InvalidSummarizerException(
                    f'Class {cls.__name__} cannot summarize result set. Row is missing attribute: latest_commit'
                )
        return activity_level_summary