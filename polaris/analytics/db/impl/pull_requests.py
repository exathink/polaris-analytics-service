# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal
import logging
from polaris.common import db
from polaris.analytics.db.model import Repository, pull_requests, repositories
from polaris.utils.collections import dict_select
from polaris.utils.exceptions import ProcessingException
from sqlalchemy import Integer, insert, select, Column
from sqlalchemy.dialects.postgresql import UUID

logger = logging.getLogger('polaris.analytics.db.impl.pull_requests')


def import_new_pull_requests(session, repository_key, pull_request_summaries):
    inserted = 0
    if len(pull_request_summaries):
        repository = Repository.find_by_repository_key(session, repository_key)
        if repository is not None:
            pull_requests_temp = db.temp_table_from(
                pull_requests,
                table_name='pull_requests_temp',
                exclude_columns=[
                    pull_requests.c.id
                ],
                extra_columns=[
                    Column('pull_request_id', Integer),
                    Column('source_repository_key', UUID(as_uuid=True))
                ]
            )
            pull_requests_temp.create(session.connection(), checkfirst=True)
            session.connection().execute(
                insert(pull_requests_temp).values([
                    dict(
                        repository_id=repository.id,
                        **dict_select(pull_request, [
                            'key',
                            'title',
                            'description',
                            'web_url',
                            'state',
                            'created_at',
                            'updated_at',
                            'deleted_at',
                            'merge_status',
                            'merged_at',
                            'source_id',
                            'display_id',
                            'source_branch',
                            'target_branch',
                            'source_repository_key',
                            'source_branch_latest_commit'
                        ])
                    )
                    for pull_request in pull_request_summaries
                ])
            )

            # find source repository id
            session.connection().execute(
                pull_requests_temp.update().values(
                    source_repository_id=select([
                        repositories.c.id.label('source_repository_id')
                    ]).where(
                        repositories.c.key == pull_requests_temp.c.source_repository_key
                    ).limit(1)
                )
            )

            # mark existing rows by copying over current pull_request_id from
            # pull requests matching by key
            session.connection().execute(
                pull_requests_temp.update().values(
                    dict(pull_request_id=pull_requests.c.id)
                ).where(
                    pull_requests.c.key == pull_requests_temp.c.key
                )
            )

            # insert missing pull requests
            inserted = session.connection().execute(
                pull_requests.insert().from_select(
                    [
                        'key',
                        'title',
                        'description',
                        'web_url',
                        'state',
                        'created_at',
                        'updated_at',
                        'deleted_at',
                        'merge_status',
                        'merged_at',
                        'source_id',
                        'display_id',
                        'source_branch',
                        'target_branch',
                        'source_repository_id',
                        'source_branch_latest_commit',
                        'repository_id'
                    ],
                    select(
                        [
                            pull_requests_temp.c.key,
                            pull_requests_temp.c.title,
                            pull_requests_temp.c.description,
                            pull_requests_temp.c.web_url,
                            pull_requests_temp.c.state,
                            pull_requests_temp.c.created_at,
                            pull_requests_temp.c.updated_at,
                            pull_requests_temp.c.deleted_at,
                            pull_requests_temp.c.merge_status,
                            pull_requests_temp.c.merged_at,
                            pull_requests_temp.c.source_id,
                            pull_requests_temp.c.display_id,
                            pull_requests_temp.c.source_branch,
                            pull_requests_temp.c.target_branch,
                            pull_requests_temp.c.source_repository_id,
                            pull_requests_temp.c.source_branch_latest_commit,
                            pull_requests_temp.c.repository_id
                        ]
                    ).where(
                        pull_requests_temp.c.pull_request_id == None
                    )
                )
            ).rowcount
        else:
            raise ProcessingException(f"Could not find repository with key: {repository_key}")
    return dict(
        insert_count=inserted
    )


def update_pull_requests(session, repository_key, pull_request_summaries):
    updated = 0
    if len(pull_request_summaries):
        repository = Repository.find_by_repository_key(session, repository_key)
        if repository is not None:
            pull_requests_temp = db.temp_table_from(
                pull_requests,
                table_name='pull_requests_temp',
                exclude_columns=[
                    pull_requests.c.id,
                    pull_requests.c.repository_id
                ]
            )
            pull_requests_temp.create(session.connection(), checkfirst=True)
            session.connection().execute(
                insert(pull_requests_temp).values([
                    dict(
                        **dict_select(pull_request, [
                            'key',
                            'title',
                            'description',
                            'web_url',
                            'state',
                            'updated_at',
                            'deleted_at',
                            'merge_status',
                            'merged_at',
                            'source_branch_latest_commit'
                        ])
                    )
                    for pull_request in pull_request_summaries
                ])
            )

            # update pull requests
            updated = session.connection().execute(
                pull_requests.update().values(
                    title=pull_requests_temp.c.title,
                    description=pull_requests_temp.c.description,
                    web_url=pull_requests_temp.c.web_url,
                    state=pull_requests_temp.c.state,
                    updated_at=pull_requests_temp.c.updated_at,
                    deleted_at=pull_requests_temp.c.deleted_at,
                    merge_status=pull_requests_temp.c.merge_status,
                    merged_at=pull_requests_temp.c.merged_at,
                    source_branch_latest_commit=pull_requests_temp.c.source_branch_latest_commit,
                ).where(
                    pull_requests_temp.c.key == pull_requests.c.key
                )
            ).rowcount
        else:
            raise ProcessingException(f"Could not find repository with key: {repository_key}")
    return dict(
        update_count=updated
    )
