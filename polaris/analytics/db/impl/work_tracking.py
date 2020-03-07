# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging
from functools import reduce
from polaris.common import db
from polaris.utils.collections import dict_select, find
from polaris.utils.exceptions import ProcessingException

from sqlalchemy import Column, String, Integer, BigInteger, select, and_, bindparam, func, literal, or_
from sqlalchemy.dialects.postgresql import UUID, insert
from polaris.analytics.db.impl.work_item_resolver import WorkItemResolver
from polaris.analytics.db.enums import WorkItemsStateType

from polaris.analytics.db.model import \
    work_items, commits, work_items_commits as work_items_commits_table, \
    repositories, organizations, projects, projects_repositories, WorkItemsSource, Organization, Repository, \
    Commit, WorkItem, work_item_state_transitions, Project, work_items_sources, \
    work_item_delivery_cycles, work_item_delivery_cycle_durations

logger = logging.getLogger('polaris.analytics.db.work_tracking')


def resolve_repository_commit_mapping_scope_from_repositories(session, organization_key, repository_summaries):
    # This is the mirror of the method below for work_items_sources that are mapped to these
    # repositories using 'repository' commit mapping scope. They cannot be commit mapped until we
    # know the repository key and so we try and match them up by source id at this point.

    work_items_sources_mapped = []
    organization = Organization.find_by_organization_key(session, organization_key)
    if organization is not None:

        for work_items_source in organization.work_items_sources:
            if work_items_source.commit_mapping_scope == 'repository' and work_items_source.commit_mapping_scope_key is None:
                mapped_repository = find(
                    repository_summaries,
                    lambda repository: repository['source_id'] == work_items_source.source_id
                )
                if mapped_repository is not None:
                    logger.info(f"Repository {mapped_repository['name']} mapped to work items source "
                                f"{work_items_source.name} for commit_mapping")
                    work_items_source.commit_mapping_scope_key = mapped_repository['key']
                    work_items_sources_mapped.append(dict(
                        work_items_source_key=work_items_source.key,
                        repository_key=mapped_repository['key']
                    ))

    return dict(
        work_items_sources_mapped=work_items_sources_mapped
    )


def resolve_commit_mapping_scope_key_for_work_items_source(session, work_items_source):
    if work_items_source.commit_mapping_scope == 'repository' and work_items_source.source_id is not None:
        # for work items sources where we have specified 'repository' as the commit_mapping_scope
        # (github is the first example) we cant know the repository key until we create the object here.
        # We try to resolve the repository key by using the source id of the repo to which this source is mapped.
        # it will succeed only if the repository exists. We have to do a similar mapping in the reverse
        # direction when a repository is created - ie match it up to work_items_sources that may be mapped to it via
        # commit_mapping_scope. This is the mirror method above
        repository = Repository.find_by_source_id(session, work_items_source.source_id)
        if repository is not None:
            logger.info(f"Mapped repository {repository.name} to "
                        f"work items source {work_items_source.name} as commit mapping scope")

            work_items_source.commit_mapping_scope_key = repository.key


def register_work_items_source(session, organization_key, work_items_source_summary):
    work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_summary[
        'key'])

    created = False
    if work_items_source is None:
        organization = Organization.find_by_organization_key(session, organization_key)
        if organization is not None:
            work_items_source = WorkItemsSource(
                key=work_items_source_summary['key'],
                name=work_items_source_summary['name'],
                organization_key=organization.key,
                integration_type=work_items_source_summary['integration_type'],
                work_items_source_type=work_items_source_summary['work_items_source_type'],
                commit_mapping_scope=work_items_source_summary['commit_mapping_scope'],
                commit_mapping_scope_key=work_items_source_summary.get('commit_mapping_scope_key'),
                source_id=work_items_source_summary.get('source_id'),
            )

            if work_items_source.commit_mapping_scope_key is None:
                resolve_commit_mapping_scope_key_for_work_items_source(session, work_items_source)

            organization.work_items_sources.append(work_items_source)
            created = True
        else:
            raise ProcessingException(f'No organization found for organization key {organization_key}')

    return dict(
        created=created,
        organization_key=organization_key,
        work_items_source=work_items_source_summary
    )


def import_new_work_items(session, work_items_source_key, work_item_summaries):
    inserted = 0
    if len(work_item_summaries) > 0:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        if work_items_source is not None:
            work_items_temp = db.temp_table_from(
                work_items,
                table_name='work_items_temp',
                exclude_columns=[
                    work_items.c.id
                ],
                extra_columns=[
                    Column('work_item_id', Integer)
                ]
            )
            work_items_temp.create(session.connection(), checkfirst=True)

            # Update the completed at date for each work item based on the state map of the work_items_source
            work_item_summaries = update_work_item_calculated_fields(work_items_source, work_item_summaries)

            session.connection().execute(
                insert(work_items_temp).values([
                    dict(
                        work_items_source_id=work_items_source.id,
                        **dict_select(work_item, [
                            'key',
                            'display_id',
                            'work_item_type',
                            'url',
                            'name',
                            'description',
                            'is_bug',
                            'tags',
                            'state',
                            'state_type',
                            'created_at',
                            'updated_at',
                            'completed_at',
                            'source_id'
                        ])
                    )
                    for work_item in work_item_summaries
                ])
            )
            # mark existing rows by copying over current work_item_id from
            # work items matching by key
            session.connection().execute(
                work_items_temp.update().values(
                    dict(work_item_id=work_items.c.id)
                ).where(
                    work_items.c.key == work_items_temp.c.key
                )
            )
            # insert missing work_items.
            inserted = session.connection().execute(
                work_items.insert().from_select(
                    [
                        'key',
                        'display_id',
                        'work_item_type',
                        'url',
                        'name',
                        'description',
                        'is_bug',
                        'tags',
                        'state',
                        'state_type',
                        'created_at',
                        'updated_at',
                        'completed_at',
                        'work_items_source_id',
                        'source_id',
                        'next_state_seq_no'
                    ],
                    select(
                        [
                            work_items_temp.c.key,
                            work_items_temp.c.display_id,
                            work_items_temp.c.work_item_type,
                            work_items_temp.c.url,
                            work_items_temp.c.name,
                            work_items_temp.c.description,
                            work_items_temp.c.is_bug,
                            work_items_temp.c.tags,
                            work_items_temp.c.state,
                            work_items_temp.c.state_type,
                            work_items_temp.c.created_at,
                            work_items_temp.c.updated_at,
                            work_items_temp.c.completed_at,
                            work_items_temp.c.work_items_source_id,
                            work_items_temp.c.source_id,
                            # We initialize the next state seq no as 2 since
                            # the seq_no 0 and 1 will be taken up by the initial states which
                            # we create below. Subsequent state changes will use
                            # the current value of the next_state_seq_no to set its sequence number.
                            literal('2').label('next_state_sequence_no')
                        ]
                    ).where(
                        work_items_temp.c.work_item_id == None
                    )
                )
            ).rowcount
            # add the created state to the state transitions
            # for the newly inserted entries.
            session.connection().execute(
                work_item_state_transitions.insert().from_select(
                    ['work_item_id', 'seq_no', 'state', 'created_at'],
                    select([
                        work_items.c.id,
                        literal('0').label('seq_no'),
                        literal('created').label('state'),
                        work_items.c.created_at
                    ]).where(
                        and_(
                            work_items.c.key == work_items_temp.c.key,
                            work_items_temp.c.work_item_id == None
                        )
                    )
                )
            )

            session.connection().execute(
                work_item_state_transitions.insert().from_select(
                    ['work_item_id', 'seq_no', 'previous_state', 'state', 'created_at'],
                    select([
                        work_items.c.id,
                        literal('1').label('seq_no'),
                        literal('created').label('previous_state'),
                        work_items.c.state,
                        # we will record the last updated date of the work_item as the state
                        # transition date since this is the closest best guess of when the actual
                        # state transition was recorded. we still have the created date on the work_item and the
                        # last updated date on the work_item might continue to get updated, so we can freeze this date
                        # as the state transition date for the state that the item was in at the moment it was imported.
                        work_items.c.updated_at
                    ]).where(
                        and_(
                            work_items.c.key == work_items_temp.c.key,
                            work_items_temp.c.work_item_id == None
                        )
                    )
                )
            )

            session.connection().execute(
                work_item_delivery_cycles.insert().from_select([
                    'work_item_id',
                    'start_seq_no',
                    'start_date',
                    ],
                    select([
                        work_items.c.id.label('work_item_id'),
                        literal('0').label('start_seq_no'),
                        work_items_temp.c.created_at.label('start_date'),
                    ]).where(
                        and_(
                            work_items_temp.c.key == work_items.c.key,
                            work_items_temp.c.work_item_id == None,
                            or_(
                                work_items_temp.c.state_type == WorkItemsStateType.backlog.value,
                                work_items_temp.c.state_type == WorkItemsStateType.open.value
                            )
                        )
                    )
                )
            )
        # delivery_cycle_fields = (work_item_id, start_seq_no, end_seq_no, start_date, end_date, lead_time, delivery_cycle_id)
        # work_item_id = work_items_temp.work_item_id
        # start_seq_no = literal('0') or work_items_state_transitions.c.seq_no where previous_state = closed and state_type = backlog/open
        # start_date = created_at
        # end_seq_no = work_item_state_transitions.seq_no when work_items_temp.state_type == 'closed'
        # end_date = work_items_temp.updated_at when work_items_temp.state_type == 'complete' or 'closed'
        # lead_time = end_date - start_date when work_items_temp.state_type == 'complete' or 'closed'
        # create delivery cycles
        # update work_items.current_delivery_cycle_id
        else:
            raise ProcessingException(f"Could not find work items source with key: {work_items_source_key}")

    return dict(
        insert_count=inserted
    )


def update_work_item_calculated_fields(work_items_source, work_item_summaries):
    # In the context of lead time, completed_at should be the 'closed'/'accepted' state only
    # Github equivalent 'closed', pivotal equivalent 'accepted', common jira equivalent 'closed'
    # state_type='closed'
    return [
        dict(
            state_type=work_items_source.get_state_type(work_item['state']),
            completed_at=work_item['updated_at']
            if work_items_source.get_state_type(work_item['state']) == WorkItemsStateType.closed.value else None,
            **work_item
        )
        for work_item in work_item_summaries
    ]


# ---------------------------
# Map commits to work items
# -----------------------------

def get_commits_query(work_items_source):
    mapping_scope = work_items_source.commit_mapping_scope

    output_cols = [
        commits.c.id,
        commits.c.key,
        commits.c.commit_message,
        commits.c.created_on_branch,
        repositories.c.key.label('repository_key')
    ]
    if mapping_scope == 'organization':
        return select(
            output_cols
        ).select_from(
            organizations.join(
                repositories, repositories.c.organization_id == organizations.c.id
            ).join(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                organizations.c.key == bindparam('commit_mapping_scope_key'),
                commits.c.author_date >= bindparam('earliest_created')
            )
        )
    elif mapping_scope == 'project':
        return select(
            output_cols
        ).distinct() \
            .select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('commit_mapping_scope_key'),
                commits.c.author_date >= bindparam('earliest_created')
            )
        )
    elif mapping_scope == 'repository':
        return select(
            output_cols
        ).select_from(
            repositories.join(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                repositories.c.key == bindparam('commit_mapping_scope_key'),
                commits.c.author_date >= bindparam('earliest_created')
            )
        )


def resolve_display_id_commits(commits_batch, integration_type, input_display_id_to_key_map):
    resolver = WorkItemResolver.get_resolver(integration_type)
    assert resolver, f"No work item resolver registered for integration type {integration_type}"
    resolved = []
    for commit in commits_batch:
        display_ids = resolver.resolve(commit.commit_message, commit.created_on_branch)
        if len(display_ids) > 0:
            for display_id in display_ids:
                if display_id in input_display_id_to_key_map:
                    resolved.append(dict(
                        commit_id=commit.id,
                        commit_key=commit.key,
                        repository_key=commit.repository_key,
                        work_item_key=input_display_id_to_key_map[display_id]
                    ))

    return resolved


map_display_ids_to_commits_page_size = 1000


def map_display_ids_to_commits(session, work_item_summaries, work_items_source):
    commit_query = get_commits_query(work_items_source)
    earliest_created = reduce(
        lambda earliest, work_item: min(earliest, work_item['created_at']),
        work_item_summaries,
        work_item_summaries[0]['created_at']
    )
    # first get a total so we can paginate through commits
    total = session.connection().execute(
        select([func.count()]).select_from(
            commit_query.alias('T')
        ), dict(
            commit_mapping_scope_key=work_items_source.commit_mapping_scope_key,
            earliest_created=earliest_created
        )
    ).scalar()

    input_display_id_to_key_map = {work_item['display_id']: work_item['key'] for work_item in work_item_summaries}
    resolved = []

    fetched = 0
    batch_size = map_display_ids_to_commits_page_size
    offset = 0
    while fetched < total:
        commits_batch = session.connection().execute(
            commit_query.limit(batch_size).offset(offset),
            dict(
                commit_mapping_scope_key=work_items_source.commit_mapping_scope_key,
                earliest_created=earliest_created
            )
        ).fetchall()

        resolved.extend(resolve_display_id_commits(
            commits_batch,
            work_items_source.integration_type,
            input_display_id_to_key_map,
        ))
        offset = offset + batch_size
        fetched = fetched + len(commits_batch)

    return resolved


def resolve_commits_for_work_items(session, organization_key, work_items_source_key, work_item_summaries):
    if len(work_item_summaries) > 0:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        if work_items_source is not None:
            resolved = map_display_ids_to_commits(session, work_item_summaries, work_items_source)
            if len(resolved) > 0:
                wc_temp = db.create_temp_table('work_items_commits_temp', [
                    Column('commit_id', BigInteger),
                    Column('work_item_key', UUID(as_uuid=True)),
                    Column('repository_key', UUID(as_uuid=True))
                ])
                wc_temp.create(session.connection(), checkfirst=True)
                session.connection().execute(
                    wc_temp.insert().values([
                        dict_select(
                            rel, [
                                'commit_id',
                                'work_item_key',
                                'repository_key'
                            ]
                        )
                        for rel in resolved
                    ])
                )

                session.connection().execute(
                    insert(work_items_commits_table).from_select(
                        ['work_item_id', 'commit_id'],
                        select([
                            work_items.c.id.label('work_item_id'),
                            wc_temp.c.commit_id
                        ]).select_from(
                            wc_temp.join(
                                work_items, wc_temp.c.work_item_key == work_items.c.key
                            )
                        )
                    ).on_conflict_do_nothing(
                        index_elements=['work_item_id', 'commit_id']
                    )
                )

            return dict(
                resolved=[
                    dict(
                        work_item_key=str(wic['work_item_key']),
                        commit_key=str(wic['commit_key']),
                        repository_key=str(wic['repository_key']),
                        work_items_source_key=str(work_items_source.key)
                    )
                    for wic in resolved
                ]
            )


# -----------------------------------------------
#
# ------------------------------------------------

def update_work_items_commits(organization_key, repository_name, work_items_commits):
    if len(work_items_commits) > 0:
        with db.create_session() as session:
            wc_temp = db.temp_table_from(
                commits,
                table_name='work_item_commits_temp',
                exclude_columns=[commits.c.id],
                extra_columns=[
                    Column('work_item_key', UUID(as_uuid=True)),
                    Column('commit_id', BigInteger)
                ]
            )
            wc_temp.create(session.connection, checkfirst=True)

            # insert tuples in form (work_item_key, commit_summary*) into the temp table.
            # the same commit might appear more than once in this table.
            session.connection.execute(
                wc_temp.insert([
                    dict(
                        work_item_key=work_item['work_item_key'],
                        repository_name=repository_name,
                        organization_key=organization_key,
                        **commit_summary
                    )
                    for work_item in work_items_commits
                    for commit_summary in work_item['commit_headers']
                ])
            )
            # extract distinct commits that dont exist in the commits table
            # and insert them
            session.connection.execute(
                commits.insert().from_select(
                    [
                        'commit_key',
                        'repository_name',
                        'organization_key',
                        'commit_date',
                        'commit_date_tz_offset',
                        'committer_contributor_name',
                        'committer_contributor_key',
                        'author_date',
                        'author_date_tz_offset',
                        'author_contributor_name',
                        'author_contributor_key',
                        'commit_message',
                        'created_on_branch',
                        'stats',
                        'parents',
                        'created_at'
                    ],
                    select(
                        [
                            wc_temp.c.commit_key,
                            wc_temp.c.repository_name,
                            wc_temp.c.organization_key,
                            wc_temp.c.commit_date,
                            wc_temp.c.commit_date_tz_offset,
                            wc_temp.c.committer_contributor_name,
                            wc_temp.c.committer_contributor_key,
                            wc_temp.c.author_date,
                            wc_temp.c.author_date_tz_offset,
                            wc_temp.c.author_contributor_name,
                            wc_temp.c.author_contributor_key,
                            wc_temp.c.commit_message,
                            wc_temp.c.created_on_branch,
                            wc_temp.c.stats,
                            wc_temp.c.parents,
                            wc_temp.c.created_at
                        ]
                    ).distinct().select_from(
                        wc_temp.outerjoin(
                            commits,
                            and_(
                                wc_temp.c.commit_key == commits.c.commit_key,
                                wc_temp.c.repository_name == commits.c.repository_name,
                                wc_temp.c.organization_key == commits.c.organization_key
                            )
                        )
                    ).where(
                        commits.c.id == None
                    )
                )
            )

            # Copy over the commit ids of all commits we are importing into the wc_temp table.
            session.connection.execute(
                wc_temp.update().values(
                    commit_id=select([
                        commits.c.id.label('commit_id')
                    ]).where(
                        and_(
                            commits.c.repository_name == wc_temp.c.repository_name,
                            commits.c.commit_key == wc_temp.c.commit_key,
                            commits.c.organization_key == wc_temp.c.organization_key
                        )
                    ).limit(1)
                )
            )

            # Now insert the work_item_commits relationships ignoring any that might already exist.
            insert_stmt = insert(work_items_commits_table).from_select(
                ['work_item_id', 'commit_id'],
                select(
                    [
                        work_items.c.id.label('work_item_id'),
                        wc_temp.c.commit_id
                    ]
                ).select_from(
                    work_items.join(
                        wc_temp, wc_temp.c.work_item_key == work_items.c.key
                    )
                )

            )

            updated_count = session.connection.execute(
                insert_stmt.on_conflict_do_nothing(
                    index_elements=['work_item_id', 'commit_id']
                )
            )
            return updated_count


def resolve_display_ids(session, display_id_commits):
    dic_temp = db.create_temp_table(
        'display_id_commits_temp', [
            Column('display_id', String),
            Column('commit_key', UUID),
            Column('commit_id', BigInteger),
            Column('work_item_id', BigInteger),
            Column('work_item_key', UUID)
        ]
    )
    dic_temp.create(session.connection(), checkfirst=True)
    session.connection().execute(
        dic_temp.insert().values(display_id_commits)
    )
    session.connection().execute(
        dic_temp.update(dict(

        ))
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


def update_commits_work_items(session, repository_key, commits_display_id):
    cdi_temp = db.create_temp_table(
        'commits_display_ids_temp', [
            Column('work_items_source_key', UUID(as_uuid=True)),
            Column('work_items_source_id', Integer),
            Column('repository_id', Integer),
            Column('source_commit_id', String),
            Column('commit_key', UUID(as_uuid=True)),
            Column('display_id', String),
            Column('commit_id', BigInteger),
            Column('work_item_id', BigInteger),
            Column('work_item_key', UUID(as_uuid=True))
        ]
    )
    cdi_temp.create(session.connection(), checkfirst=True)

    session.connection().execute(
        cdi_temp.insert().values(commits_display_id)
    )

    session.connection().execute(
        cdi_temp.update().where(
            and_(
                commits.c.repository_id == cdi_temp.c.repository_id,
                commits.c.source_commit_id == cdi_temp.c.source_commit_id
            )
        ).values(
            commit_id=commits.c.id
        )
    )

    session.connection().execute(
        cdi_temp.update().where(
            and_(
                work_items.c.work_items_source_id == cdi_temp.c.work_items_source_id,
                work_items.c.display_id == cdi_temp.c.display_id
            )
        ).values(
            work_item_key=work_items.c.key,
            work_item_id=work_items.c.id
        )
    )

    session.connection().execute(
        insert(work_items_commits_table).from_select(
            ['work_item_id', 'commit_id'],
            select([cdi_temp.c.work_item_id, cdi_temp.c.commit_id]).where(
                cdi_temp.c.work_item_id != None
            )
        ).on_conflict_do_nothing(
            index_elements=['work_item_id', 'commit_id']
        )
    )
    return [
        dict(
            commit_key=str(row.commit_key),
            work_item_key=str(row.work_item_key),
            work_items_source_key=str(row.work_items_source_key),
            repository_key=str(repository_key),
        )
        for row in session.connection().execute(
            select([
                cdi_temp.c.work_item_key,
                cdi_temp.c.commit_key,
                cdi_temp.c.work_items_source_key
            ]).where(
                cdi_temp.c.work_item_id != None
            ).distinct()
        ).fetchall()
    ]


def resolve_work_items_for_commits(session, organization_key, repository_key, commit_summaries):
    resolved = []
    repository = Repository.find_by_repository_key(session, repository_key)
    if repository is not None:
        work_items_sources = find_work_items_sources(session, organization_key, repository_key)
        if len(work_items_sources) > 0:
            commits_display_ids = []
            for work_items_source in work_items_sources:
                work_item_resolver = WorkItemResolver.get_resolver(work_items_source.integration_type)
                for commit in commit_summaries:
                    for display_id in work_item_resolver.resolve(commit['commit_message'], commit['created_on_branch']):
                        commits_display_ids.append(
                            dict(
                                repository_id=repository.id,
                                source_commit_id=commit['source_commit_id'],
                                commit_key=commit['key'],
                                work_items_source_id=work_items_source.id,
                                work_items_source_key=work_items_source.key,
                                display_id=display_id
                            )
                        )

            resolved = update_commits_work_items(session, repository_key, commits_display_ids)

    return dict(
        resolved=resolved
    )


def update_commit_work_item_summaries(session, organization_key, work_item_commits):
    updated_commits = {}
    work_items_to_add = {}

    for entry in work_item_commits:
        commit_key = entry['commit_key']
        commit = updated_commits.get(commit_key)
        if commit is None:
            commit = Commit.find_by_commit_key(session, commit_key)
            updated_commits[commit_key] = commit

        work_item_key = entry['work_item_key']
        work_item = work_items_to_add.get(work_item_key)
        if work_item is None:
            work_item = WorkItem.find_by_work_item_key(session, work_item_key)
            work_items_to_add[work_item_key] = work_item

        commit.add_work_item_summary(work_item.get_summary())

    return dict()


def update_work_items(session, work_items_source_key, work_item_summaries):
    updated = 0
    if len(work_item_summaries) > 0:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        if work_items_source is not None:
            work_items_temp = db.temp_table_from(
                work_items,
                table_name='work_items_temp',
                exclude_columns=[
                    work_items.c.id,
                    work_items.c.work_items_source_id
                ]
            )
            work_items_temp.create(session.connection(), checkfirst=True)

            work_item_summaries = update_work_item_calculated_fields(work_items_source, work_item_summaries)

            session.connection().execute(
                insert(work_items_temp).values([
                    dict_select(
                        work_item, [
                            'key',
                            'name',
                            'url',
                            'description',
                            'is_bug',
                            'tags',
                            'state',
                            'state_type',
                            'updated_at',
                            'completed_at'
                        ]
                    )
                    for work_item in work_item_summaries
                ]
                ))
            state_changes = db.row_proxies_to_dict(
                session.connection().execute(
                    select([
                        work_items.c.key,
                        work_items.c.id.label('work_item_id'),
                        work_items.c.state.label('previous_state'),
                        work_items.c.next_state_seq_no.label('seq_no'),
                        work_items_temp.c.state.label('state'),
                        # update timestamp is the create timestamp for state change
                        work_items_temp.c.updated_at.label('created_at'),
                    ]).where(
                        and_(
                            work_items.c.key == work_items_temp.c.key,
                            work_items.c.state != work_items_temp.c.state
                        )
                    )
                ).fetchall()
            )

            if len(state_changes) > 0:
                # Insert the new state transition row for the change
                session.connection().execute(
                    work_item_state_transitions.insert().values([
                        dict_select(change, [
                            'work_item_id',
                            'seq_no',
                            'previous_state',
                            'state',
                            'created_at'
                        ])
                        for change in state_changes
                    ]
                    )
                )
                # Update the next_state_seq_no counter for all rows that have state changes.
                session.connection().execute(
                    work_items.update().values(
                        next_state_seq_no=work_items.c.next_state_seq_no + 1
                    ).where(
                        and_(
                            work_items_temp.c.key == work_items.c.key,
                            work_items.c.state != work_items_temp.c.state
                        )
                    )
                )

            # finally do the update of the changed rows.
            updated = session.connection().execute(
                work_items.update().values(
                    url=work_items_temp.c.url,
                    name=work_items_temp.c.name,
                    description=work_items_temp.c.description,
                    is_bug=work_items_temp.c.is_bug,
                    tags=work_items_temp.c.tags,
                    state=work_items_temp.c.state,
                    state_type=work_items_temp.c.state_type,
                    updated_at=work_items_temp.c.updated_at,
                    completed_at=work_items_temp.c.completed_at
                ).where(
                    work_items_temp.c.key == work_items.c.key
                )
            ).rowcount



        else:
            raise ProcessingException(f"Could not find work items source with key: {work_items_source_key}")

        return dict(
            update_count=updated,
            state_changes=state_changes
        )


def import_project(session, organization_key, project_summary):
    organization = Organization.find_by_organization_key(session, organization_key)
    if organization is not None:
        project = Project.find_by_project_key(session, project_summary['key'])
        if project is None:
            project = Project(
                key=project_summary['key'],
                name=project_summary['name']
            )
            organization.projects.append(project)
            session.add(project)
        new_work_items_sources = []
        for source in project_summary['work_items_sources']:
            work_items_source = WorkItemsSource.find_by_work_items_source_key(session, source['key'])

            if work_items_source is None:
                work_items_source = WorkItemsSource(
                    organization_key=organization.key,
                    **source
                )
                work_items_source.init_state_map()

                if work_items_source.commit_mapping_scope_key is None:
                    resolve_commit_mapping_scope_key_for_work_items_source(session, work_items_source)

                organization.work_items_sources.append(work_items_source)
                project.work_items_sources.append(work_items_source)
                new_work_items_sources.append(work_items_source)

            elif not find(project.work_items_sources, lambda w: w.key == work_items_source.key):
                project.work_items_sources.append(work_items_source)

        return dict(
            new_work_items_sources=len(new_work_items_sources)
        )

    else:
        raise ProcessingException(f'Could not find organization with key {organization_key}')


def infer_projects_repositories_relationships(session, organization_key, work_items_commits):
    wisr_temp = db.create_temp_table(
        'work_items_sources_repositories_temp', [
            Column('work_items_source_key', UUID(as_uuid=True)),
            Column('repository_key', UUID(as_uuid=True))
        ]
    )
    wisr_temp.create(session.connection(), checkfirst=True)

    session.connection().execute(
        wisr_temp.insert().values(
            [
                dict_select(
                    work_items_commit,
                    [
                        'work_items_source_key',
                        'repository_key'
                    ]
                )
                for work_items_commit in work_items_commits
            ]
        )
    )

    projects_repositories_relationships = select([
        projects.c.key.label('project_key'),
        repositories.c.key.label('repository_key'),
        projects.c.id.label('project_id'),
        repositories.c.id.label('repository_id')
    ]).select_from(
        wisr_temp.join(
            repositories, wisr_temp.c.repository_key == repositories.c.key
        ).join(
            work_items_sources, wisr_temp.c.work_items_source_key == work_items_sources.c.key
        ).join(
            projects, work_items_sources.c.project_id == projects.c.id
        )
    ).distinct().alias()

    new_relationships_query = select([
        projects_repositories_relationships.c.project_key,
        projects_repositories_relationships.c.repository_key,
        projects_repositories_relationships.c.project_id,
        projects_repositories_relationships.c.repository_id
    ]).select_from(
        projects_repositories_relationships.outerjoin(
            projects_repositories,
            and_(
                projects_repositories_relationships.c.project_id == projects_repositories.c.project_id,
                projects_repositories_relationships.c.repository_id == projects_repositories.c.repository_id
            )
        )
    ).where(
        or_(
            projects_repositories.c.project_id == None,
            projects_repositories.c.repository_id == None
        )
    ).alias()

    new_relationships = session.connection().execute(
        new_relationships_query
    ).fetchall()

    if len(new_relationships) > 0:
        session.connection().execute(
            insert(projects_repositories).from_select(
                ['project_id', 'repository_id'],
                select([new_relationships_query.c.project_id, new_relationships_query.c.repository_id])
            )
        )

    return dict(
        new_relationships=[
            dict(
                project_key=str(relationship.project_key),
                repository_key=str(relationship.repository_key)
            )
            for relationship in new_relationships
        ]
    )
