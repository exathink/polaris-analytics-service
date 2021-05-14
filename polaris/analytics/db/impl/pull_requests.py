# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import logging

from functools import reduce
from polaris.common import db
from polaris.analytics.db.model import organizations, pull_requests, repositories, projects, projects_repositories, \
    Repository, WorkItemsSource, work_items, work_items_pull_requests as work_items_pull_requests_table
from polaris.analytics.db.impl.work_item_resolver import WorkItemResolver
from polaris.utils.collections import dict_select
from polaris.utils.exceptions import ProcessingException
from sqlalchemy import Integer, select, Column, bindparam, String, BigInteger, and_, func
from sqlalchemy.dialects.postgresql import UUID, insert, array

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
                            'source_state',
                            'state',
                            'created_at',
                            'updated_at',
                            'deleted_at',
                            'merge_status',
                            'end_date',
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
                        'source_state',
                        'state',
                        'created_at',
                        'updated_at',
                        'deleted_at',
                        'merge_status',
                        'end_date',
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
                            pull_requests_temp.c.source_state,
                            pull_requests_temp.c.state,
                            pull_requests_temp.c.created_at,
                            pull_requests_temp.c.updated_at,
                            pull_requests_temp.c.deleted_at,
                            pull_requests_temp.c.merge_status,
                            pull_requests_temp.c.end_date,
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
                            'source_state',
                            'state',
                            'updated_at',
                            'deleted_at',
                            'merge_status',
                            'end_date'
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
                    source_state=pull_requests_temp.c.source_state,
                    state=pull_requests_temp.c.state,
                    updated_at=pull_requests_temp.c.updated_at,
                    deleted_at=pull_requests_temp.c.deleted_at,
                    merge_status=pull_requests_temp.c.merge_status,
                    end_date=pull_requests_temp.c.end_date
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
        pull_requests.c.display_id,
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
            and_(
                organizations.c.key == bindparam('pull_request_mapping_scope_key'),
                pull_requests.c.created_at >= bindparam('earliest_created')
            )

        )
    elif mapping_scope == 'project':
        return select(
            output_cols
        ).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                pull_requests, pull_requests.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('pull_request_mapping_scope_key'),
                pull_requests.c.created_at >= bindparam('earliest_created')
            )
        )
    elif mapping_scope == 'repository':
        return select(
            output_cols
        ).select_from(
            repositories.join(
                pull_requests, pull_requests.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                repositories.c.key == bindparam('pull_request_mapping_scope_key'),
                pull_requests.c.created_at >= bindparam('earliest_created')
            )
        )


def resolve_pull_request_identifiers_pull_requests(pull_requests_batch, integration_type,
                                                   pull_request_identifiers_to_key_map):
    resolver = WorkItemResolver.get_resolver(integration_type)
    assert resolver, f"No work item resolver registered for integration type {integration_type}"
    resolved = []
    for pr in pull_requests_batch:
        pull_request_identifiers = resolver.resolve(pr.title, pr.description, display_id=pr.display_id,
                                                    branch_name=pr.source_branch)
        if len(pull_request_identifiers) > 0:
            for pull_request_identifier in pull_request_identifiers:
                if pull_request_identifier in pull_request_identifiers_to_key_map:
                    resolved.append(dict(
                        pull_request_id=pr.id,
                        pull_request_key=pr.key,
                        repository_key=pr.repository_key,
                        work_item_key=pull_request_identifiers_to_key_map[pull_request_identifier]['key']
                    ))

    return resolved


map_pull_request_identifiers_to_pull_requests_page_size = 1000


def map_pull_request_identifiers_to_pull_requests(session, work_item_summaries, work_items_source):
    # get pull requests query for given work_item_source/repository
    # skipping pagination in first go, as PRs will be much lesser than commits
    pull_request_query = get_pull_requests_query(work_items_source)
    earliest_created = reduce(
        lambda earliest, work_item: min(earliest, work_item['created_at']),
        work_item_summaries,
        work_item_summaries[0]['created_at']
    )
    # first get a total so we can paginate through commits
    total = session.connection().execute(
        select([func.count()]).select_from(
            pull_request_query.alias('T')
        ), dict(
            pull_request_mapping_scope_key=work_items_source.commit_mapping_scope_key,
            earliest_created=earliest_created
        )
    ).scalar()

    pull_request_identifiers_to_key_map = {
        work_item['display_id']: {'key': work_item['key'], 'created_at': work_item['created_at']} for work_item in
        work_item_summaries}
    for work_item in work_item_summaries:
        if work_item.get('commit_identifiers') != None:
            if work_item.get('commit_identifiers') != []:
                for commit_identifier in work_item.get('commit_identifiers'):
                    pull_request_identifiers_to_key_map.update(
                        {commit_identifier: {'key': work_item['key'], 'created_at': work_item['created_at']}})
    resolved = []

    fetched = 0
    batch_size = map_pull_request_identifiers_to_pull_requests_page_size
    offset = 0
    while fetched < total:
        pull_requests_batch = session.connection().execute(
            pull_request_query.limit(batch_size).offset(offset),
            dict(
                pull_request_mapping_scope_key=work_items_source.commit_mapping_scope_key,
                earliest_created=earliest_created
            )
        ).fetchall()

        resolved.extend(
            resolve_pull_request_identifiers_pull_requests(
                pull_requests_batch,
                work_items_source.integration_type,
                pull_request_identifiers_to_key_map
            )
        )
        offset = offset + batch_size
        fetched = fetched + len(pull_requests_batch)

    return resolved


def resolve_pull_requests_for_work_items(session, organization_key, work_items_source_key, work_item_summaries):
    if len(work_item_summaries) > 0:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        if work_items_source is not None:
            resolved = map_pull_request_identifiers_to_pull_requests(session, work_item_summaries, work_items_source)
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
                        ['work_item_id', 'pull_request_id', 'delivery_cycle_id'],
                        select([
                            work_items.c.id.label('work_item_id'),
                            wp_temp.c.pull_request_id,
                            work_items.c.current_delivery_cycle_id
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


def find_work_items_sources(session, organization_key, repository_key):
    """
    Work Item Sources are searched for from the most specific to the most general until the first one is found.
    If there is a work item source mapped to the repository only that is searched. Else if there are work items sources
    mapped to a project that repository belongs to they are returned. Else any work items sources mapped to the organization
    are returned.

    :param session:
    :param organization_key:
    :param repository_key:
    :return:
    """
    work_item_sources = WorkItemsSource.find_by_commit_mapping_scope(
        session,
        organization_key,
        commit_mapping_scope='repository',
        commit_mapping_scope_keys=[repository_key]
    )
    if len(work_item_sources) == 0:
        repository = Repository.find_by_repository_key(session, repository_key)
        if len(repository.projects) > 0:
            work_item_sources = WorkItemsSource.find_by_commit_mapping_scope(
                session,
                organization_key,
                commit_mapping_scope='project',
                commit_mapping_scope_keys=[project.key for project in repository.projects]
            )

        if len(work_item_sources) == 0:
            work_item_sources = WorkItemsSource.find_by_commit_mapping_scope(
                session,
                organization_key,
                commit_mapping_scope='organization',
                commit_mapping_scope_keys=[organization_key]
            )

    return work_item_sources


def update_pull_requests_work_items(session, repository_key, pull_requests_pull_request_identifiers):
    pdi_temp = db.create_temp_table(
        'pull_requests_pull_request_identifiers_temp', [
            Column('work_items_source_key', UUID(as_uuid=True)),
            Column('work_items_source_id', Integer),
            Column('repository_id', Integer),
            Column('source_pull_request_id', String),
            Column('pull_request_key', UUID(as_uuid=True)),
            Column('pull_request_identifier', String),
            Column('pull_request_id', BigInteger),
            Column('work_item_id', BigInteger),
            Column('work_item_key', UUID(as_uuid=True)),
            Column('delivery_cycle_id', Integer)
        ]
    )
    pdi_temp.create(session.connection(), checkfirst=True)

    session.connection().execute(
        pdi_temp.insert().values(pull_requests_pull_request_identifiers)
    )

    session.connection().execute(
        pdi_temp.update().where(
            and_(
                pull_requests.c.repository_id == pdi_temp.c.repository_id,
                pull_requests.c.source_id == pdi_temp.c.source_pull_request_id
            )
        ).values(
            pull_request_id=pull_requests.c.id
        )
    )

    session.connection().execute(
        pdi_temp.update().where(
            and_(
                work_items.c.work_items_source_id == pdi_temp.c.work_items_source_id,
                work_items.c.display_id == pdi_temp.c.pull_request_identifier
            )
        ).values(
            work_item_key=work_items.c.key,
            work_item_id=work_items.c.id,
            delivery_cycle_id=work_items.c.current_delivery_cycle_id
        )
    )

    session.connection().execute(
        pdi_temp.update().where(
            and_(
                work_items.c.work_items_source_id == pdi_temp.c.work_items_source_id,
                work_items.c.commit_identifiers.has_any(array([pdi_temp.c.pull_request_identifier]))
            )
        ).values(
            work_item_key=work_items.c.key,
            work_item_id=work_items.c.id,
            delivery_cycle_id=work_items.c.current_delivery_cycle_id
        )
    )

    session.connection().execute(
        insert(work_items_pull_requests_table).from_select(
            ['work_item_id', 'pull_request_id', 'delivery_cycle_id'],
            select([pdi_temp.c.work_item_id, pdi_temp.c.pull_request_id, pdi_temp.c.delivery_cycle_id]).where(
                pdi_temp.c.work_item_id != None
            )
        ).on_conflict_do_nothing(
            index_elements=['work_item_id', 'pull_request_id']
        )
    )
    return [
        dict(
            pull_request_key=str(row.pull_request_key),
            work_item_key=str(row.work_item_key),
            work_items_source_key=str(row.work_items_source_key),
            repository_key=str(repository_key),
        )
        for row in session.connection().execute(
            select([
                pdi_temp.c.work_item_key,
                pdi_temp.c.pull_request_key,
                pdi_temp.c.work_items_source_key
            ]).where(
                pdi_temp.c.work_item_id != None
            ).distinct()
        ).fetchall()
    ]


def resolve_work_items_for_pull_requests(session, organization_key, repository_key, pull_request_summaries):
    resolved = []
    repository = Repository.find_by_repository_key(session, repository_key)
    if repository is not None:
        work_items_sources = find_work_items_sources(session, organization_key, repository_key)
        if len(work_items_sources) > 0:
            prs_pull_request_identifiers = []
            for work_items_source in work_items_sources:
                work_item_resolver = WorkItemResolver.get_resolver(work_items_source.integration_type)
                for pr in pull_request_summaries:
                    for pull_request_identifier in work_item_resolver.resolve(pr['title'], pr['description'],
                                                                              display_id=pr['display_id'],
                                                                              branch_name=pr['source_branch']):
                        prs_pull_request_identifiers.append(
                            dict(
                                repository_id=repository.id,
                                source_pull_request_id=pr['source_id'],
                                pull_request_key=pr['key'],
                                work_items_source_id=work_items_source.id,
                                work_items_source_key=work_items_source.key,
                                pull_request_identifier=pull_request_identifier
                            )
                        )

            resolved = update_pull_requests_work_items(session, repository_key, prs_pull_request_identifiers)
    return dict(
        resolved=resolved
    )
