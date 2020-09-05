# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import cast, Text, func, and_, Date
from datetime import datetime, timedelta


def commit_key_column(repositories, commits):
    return (cast(repositories.c.key, Text) + ':' + cast(commits.c.source_commit_id, Text)).label('key')


def commit_name_column(commits):
    return func.substr(commits.c.source_commit_id, 1, 8).label('name')


def commit_info_columns(repositories, commits):
    return [
        commits.c.id,
        commit_key_column(repositories, commits),
        commit_name_column(commits),
        commits.c.source_commit_id.label('commit_hash'),
        repositories.c.name.label('repository'),
        repositories.c.integration_type.label('integration_type'),
        repositories.c.url.label('repository_url'),
        repositories.c.key.label('repository_key'),
        commits.c.commit_date,
        commits.c.committer_contributor_name.label('committer'),
        commits.c.committer_contributor_key.label('committer_key'),
        commits.c.author_date,
        commits.c.author_contributor_name.label('author'),
        commits.c.author_contributor_key.label('author_key'),
        commits.c.commit_message,
        commits.c.num_parents,
        commits.c.created_on_branch.label('branch'),
        commits.c.stats,
        commits.c.source_file_types_summary.label('file_types_summary'),
        commits.c.work_items_summaries
    ]


def apply_time_window_filters(select_stmt, commits_relation, **kwargs):
    before = None
    if 'before' in kwargs:
        before = kwargs['before']

    if 'days' in kwargs and kwargs['days'] > 0:
        if before:
            commit_window_start = before - timedelta(days=kwargs['days'])
            return select_stmt.where(
                and_(
                    commits_relation.c.commit_date >= commit_window_start,
                    commits_relation.c.commit_date <= before
                )
            )
        else:
            commit_window_start = datetime.utcnow() - timedelta(days=kwargs['days'])
            return select_stmt.where(
                commits_relation.c.commit_date >= commit_window_start
            )
    elif before:
        return select_stmt.where(
            commits_relation.c.commit_date <= before
        )
    else:
        return select_stmt


def commits_connection_apply_filters(select_stmt, commits_relation, **kwargs):
    select_stmt = apply_time_window_filters(select_stmt, commits_relation, **kwargs)

    return select_stmt


def coding_day(commits):
    return cast(
        func.to_timestamp(
            func.extract('epoch', commits.c.commit_date) - commits.c.commit_date_tz_offset
        ),
        Date
    )
