# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging
from functools import reduce
from polaris.common import db
from polaris.utils.collections import dict_select
from polaris.utils.exceptions import ProcessingException

from sqlalchemy import Column, String, Integer, BigInteger, select, and_, bindparam, func
from sqlalchemy.dialects.postgresql import UUID, insert
from polaris.utils.work_tracking import WorkItemResolver
from polaris.analytics.db.model import \
    work_items, commits, work_items_commits as work_items_commits_table, \
    repositories, organizations, projects, projects_repositories, WorkItemsSource, Organization, Repository, \
    Commit, WorkItem

logger = logging.getLogger('polaris.analytics.db.work_tracking')


def register_work_items_source(session, organization_key, work_items_source_summmary):
    work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_summmary[
        'key'])

    created = False
    if work_items_source is None:
        organization = Organization.find_by_organization_key(session, organization_key)
        if organization is not None:
            work_items_source = WorkItemsSource(
                key=work_items_source_summmary['key'],
                name=work_items_source_summmary['name'],
                organization_key=organization.key,
                integration_type=work_items_source_summmary['integration_type'],
                commit_mapping_scope=work_items_source_summmary['commit_mapping_scope'],
                commit_mapping_scope_key=work_items_source_summmary['commit_mapping_scope_key']
            )
            organization.work_items_sources.append(work_items_source)
            created = True
        else:
            raise ProcessingException(f'No organization found for organization key {organization_key}')

    return dict(
        created=created,
        organization_key=organization_key,
        work_items_source=work_items_source_summmary
    )


def import_new_work_items(session, work_items_source_key, work_item_summaries):
    inserted = 0
    if len(work_item_summaries) > 0:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        if work_items_source is not None:
            inserted = session.connection().execute(
                insert(work_items).values([
                    dict(
                        work_items_source_id=work_items_source.id,
                        **dict_select(work_item, [
                            'key',
                            'display_id',
                            'url',
                            'name',
                            'description',
                            'is_bug',
                            'tags',
                            'state',
                            'created_at',
                            'updated_at'
                        ])
                    )
                    for work_item in work_item_summaries
                ]).on_conflict_do_nothing(
                    index_elements=['key']
                )
            ).rowcount
        else:
            raise ProcessingException(f"Could not find work items source with key: {work_items_source_key}")

    return dict(
        insert_count=inserted
    )


# ---------------------------
# Map commits to work items
# -----------------------------

def get_commits_query(mapping_scope):
    output_cols = [
        commits.c.id,
        commits.c.key,
        commits.c.commit_message
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


def resolve_display_id_commits(commits_batch, integration_type, input_display_ids):
    resolver = WorkItemResolver.get_resolver(integration_type)
    assert resolver, f"No work item resolver registered for integration type {integration_type}"
    resolved = []
    for commit in commits_batch:
        display_ids = resolver.resolve(commit.commit_message)
        if len(display_ids) > 0:
            for display_id in display_ids:
                if display_id in input_display_ids:
                    resolved.append(dict(
                        commit_id=commit.id,
                        commit_key=commit.key,
                        work_item_key=input_display_ids[display_id]
                    ))

    return resolved


map_display_ids_to_commits_page_size = 1000


def map_display_ids_to_commits(session, work_item_summaries, work_items_source):
    commit_query = get_commits_query(work_items_source.commit_mapping_scope)
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

    input_display_ids = {work_item['display_id']: work_item['key'] for work_item in work_item_summaries}
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
            input_display_ids,
        ))
        offset = offset + batch_size
        fetched = fetched + len(commits_batch)

    return resolved


def resolve_commits_for_work_items(session, organization_key, work_items_source_key, work_item_summaries):
    resolved = []
    if len(work_item_summaries) > 0:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        if work_items_source is not None:
            resolved = map_display_ids_to_commits(session, work_item_summaries, work_items_source)
            if len(resolved) > 0:
                wc_temp = db.create_temp_table('work_items_commits_temp', [
                    Column('commit_id', BigInteger),
                    Column('work_item_key', UUID)
                ])
                wc_temp.create(session.connection(), checkfirst=True)
                session.connection().execute(
                    wc_temp.insert().values([
                        dict_select(
                            rel, [
                                'commit_id',
                                'work_item_key'
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
                    work_item_key=wic['work_item_key'],
                    commit_key=wic['commit_key'].hex
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
            Column('work_items_source_id', Integer),
            Column('repository_id', Integer),
            Column('source_commit_id', String),
            Column('commit_key', UUID),
            Column('display_id', String),
            Column('commit_id', BigInteger),
            Column('work_item_id', BigInteger),
            Column('work_item_key', UUID)
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
            commit_key=row.commit_key,
            work_item_key=row.work_item_key
        )
        for row in session.connection().execute(
            select([cdi_temp.c.work_item_key, cdi_temp.c.commit_key]).where(
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
                    for display_id in work_item_resolver.resolve(commit['commit_message']):
                        commits_display_ids.append(
                            dict(
                                repository_id=repository.id,
                                source_commit_id=commit['source_commit_id'],
                                commit_key=commit['key'],
                                work_items_source_id=work_items_source.id,
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
                            'updated_at'
                        ]
                    )
                    for work_item in work_item_summaries
                ]
            ))
            updated = session.connection().execute(
                work_items.update().values(
                    url=work_items_temp.c.url,
                    name=work_items_temp.c.name,
                    description=work_items_temp.c.description,
                    is_bug=work_items_temp.c.is_bug,
                    tags=work_items_temp.c.tags,
                    state=work_items_temp.c.state,
                    updated_at=work_items_temp.c.updated_at
                ).where(
                    work_items_temp.c.key == work_items.c.key
                )
            ).rowcount

        else:
            raise ProcessingException(f"Could not find work items source with key: {work_items_source_key}")

    return dict(
        update_count=updated
    )
