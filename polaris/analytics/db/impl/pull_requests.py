# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal
import logging
from polaris.common import db
from polaris.analytics.db.model import organizations, pull_requests, repositories, projects, projects_repositories, \
    Repository, WorkItemsSource, work_items, work_items_pull_requests as work_items_pull_requests_table
from polaris.analytics.db.impl.pull_request_work_item_resolver import PullRequestWorkItemResolver
from polaris.utils.collections import dict_select
from polaris.utils.exceptions import ProcessingException
from sqlalchemy import Integer, insert, select, Column, bindparam
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
                ).where(
                    pull_requests_temp.c.key == pull_requests.c.key
                )
            ).rowcount
        else:
            raise ProcessingException(f"Could not find repository with key: {repository_key}")
    return dict(
        update_count=updated
    )


def get_pull_requests_query(work_items_source):
    # Using commit_mapping_scope as scope for mapping pull_requests too, \
    # as it doesn't seem to have any different scope
    mapping_scope = work_items_source.commit_mapping_scope

    output_cols = [
        pull_requests.c.id,
        pull_requests.c.key,
        pull_requests.c.title,
        pull_requests.c.description,
        pull_requests.c.source_branch,
        repositories.c.key.label('repository_key')
    ]
    if mapping_scope == 'organization':
        return select(
            output_cols
        ).select_from(
            organizations.join(
                repositories, repositories.c.organization_id == organizations.c.id
            ).join(
                pull_requests, pull_requests.c.repository_id == repositories.c.id
            )
        ).where(
            organizations.c.key == bindparam('pull_request_mapping_scope_key')
        )
    elif mapping_scope == 'project':
        return select(
            output_cols
        ).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.prjoect_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                pull_requests, pull_requests.c.repository_id == repositories.c.id
            )
        ).where(
            projects.c.key == bindparam('pull_request_mapping_scope_key')
        )
    elif mapping_scope == 'repository':
        return select(
            output_cols
        ).select_from(
            repositories.join(
                pull_requests, pull_requests.c.repository_id == repositories.c.id
            ).where(
                repositories.c.key == bindparam('pull_request_mapping_scope_key')
            )
        )


def resolve_display_id_pull_requests(pull_requests_batch, integration_type, input_display_id_to_key_map):
    resolver = PullRequestWorkItemResolver.get_resolver(integration_type)
    assert resolver, f"No work item resolver registered for integration type {integration_type}"
    resolved = []
    for pr in pull_requests_batch:
        display_ids = resolver.resolve(pr.title, pr.description, pr.source_branch)
        if len(display_ids) > 0:
            for display_id in display_ids:
                if display_id in input_display_id_to_key_map:
                    resolved.append(dict(
                        pull_request_id=pr.id,
                        pull_request_key=pr.key,
                        repository_key=pr.repository_key,
                        work_item_key=input_display_id_to_key_map[display_id]
                    ))

    return resolved


def map_display_ids_to_pull_requests(session, work_item_summaries, work_items_source):
    # get pull requests query for given work_item_source/repository
    # skipping pagination in first go, as PRs will be much lesser than commits
    resolved = []
    pull_request_query = get_pull_requests_query(work_items_source)
    # Skipping pagination in first run. Will add if required later
    input_display_id_to_key_map = {work_item['display_id']: work_item['key'] for work_item in work_item_summaries}
    resolved = []
    pull_requests_batch = session.connection().execute(
        pull_request_query,
        dict(
            pull_request_mapping_scope_key=work_items_source.commit_mapping_scope_key
        )
    )
    resolved = resolve_display_id_pull_requests(
        pull_requests_batch,
        work_items_source.integration_type,
        input_display_id_to_key_map
    )

    return resolved


def resolve_pull_requests_for_work_items(session, organization_key, work_items_source_key, work_item_summaries):
    if len(work_item_summaries) > 0:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        if work_items_source is not None:
            resolved = map_display_ids_to_pull_requests(session, work_item_summaries, work_items_source)
            if len(resolved) > 0:
                wp_temp = db.create_temp_table('work_items_pull_requests_temp', [
                    Column('pull_request_id', Integer),
                    Column('work_item_key', UUID(as_uuid=True)),
                    Column('repository_key', UUID(as_uuid=True))
                ])
                wp_temp.create(session.connection(), checkfirst=True)
                session.connection().execute(
                    wp_temp.insert().values([
                        dict_select(
                            rel, [
                                'pull_request_id',
                                'work_item_key',
                                'repository_key'
                            ]
                        )
                        for rel in resolved
                    ])
                )

                session.connection().execute(
                    insert(work_items_pull_requests_table).from_select(
                        ['work_item_id', 'pull_request_id'],
                        select([
                            work_items.c.id.label('work_item_id'),
                            wp_temp.c.pull_request_id
                        ]).select_from(
                            wp_temp.join(
                                work_items, work_items.c.key == wp_temp.c.work_item_key
                            )
                        )
                    ).on_conflict_do_nothing(
                        index_elements=['work_item_id', 'pull_request_id']
                    )
                )
            return dict(
                resolved=[
                    dict(
                        work_item_key=str(wipr['work_item_key']),
                        pull_request_key=str(wipr['pull_request_key']),
                        repository_key=str(wipr['repository_key']),
                        work_items_source_key=str(work_items_source.key)
                    )
                    for wipr in resolved
                ]
            )


def resolve_work_items_for_pull_requests(session, organization_key, repository_key, commit_summaries):
    resolved = []

    return dict(
        resolved=resolved
    )