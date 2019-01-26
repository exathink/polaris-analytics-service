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

from sqlalchemy import Column, BigInteger, select, and_, bindparam, func
from sqlalchemy.dialects.postgresql import UUID, insert
from polaris.utils.work_tracking import WorkItemResolver
from polaris.analytics.db.model import \
    work_items, commits, work_items_commits as work_items_commits_table, \
    repositories, organizations, projects, projects_repositories, WorkItemsSource, Organization

logger = logging.getLogger('polaris.analytics.db.work_tracking')


def register_work_items_source(session, organization_key, work_items_source_summmary):
    work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_summmary['work_items_source_key'])
    if work_items_source is None:
        organization = Organization.find_by_organization_key(session, organization_key)
        work_items_source = WorkItemsSource(
            key=work_items_source_summmary['work_items_source_key'],
            name=work_items_source_summmary['name'],
            integration_type=work_items_source_summmary['integration_type'],
            commit_mapping_scope=work_items_source_summmary['commit_mapping_scope'],
            commit_mapping_scope_key=work_items_source_summmary['commit_mapping_scope_key']
        )
        organization.work_items_sources.append(work_items_source)

    return work_items_source


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
        repositories.c.name.label('repository_name'),
        commits.c.source_commit_id,
        commits.c.commit_date,
        commits.c.commit_date_tz_offset,
        commits.c.committer_contributor_name,
        commits.c.committer_contributor_key,
        commits.c.author_date,
        commits.c.author_date_tz_offset,
        commits.c.author_contributor_name,
        commits.c.author_contributor_key,
        commits.c.commit_message,
        commits.c.created_on_branch,
        commits.c.stats,
        commits.c.parents,
        commits.c.created_at
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
                organizations.c.organization_key == bindparam('commit_mapping_scope_key'),
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
                projects.c.project_key == bindparam('commit_mapping_scope_key'),
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


def resolve_display_id_commits_by_repo(commits_batch, integration_type, input_display_ids,
                                       resolved_display_id_commits_by_repo):
    resolver = WorkItemResolver.get_resolver(integration_type)
    assert resolver, f"No work item resolver registered for integration type {integration_type}"

    for commit in commits_batch:
        display_ids = resolver.resolve(commit.commit_message)
        if len(display_ids) > 0:
            for display_id in display_ids:

                if display_id in input_display_ids:
                    repo = commit['repository_name']
                    repo_display_id_commits = resolved_display_id_commits_by_repo.get(repo, {})
                    display_id_commits = repo_display_id_commits.get(display_id, [])
                    display_id_commits.append(
                        dict(
                            source_commit_id=commit.source_commit_id,
                            commit_date=commit.commit_date,
                            commit_date_tz_offset=commit.commit_date_tz_offset,
                            committer_contributor_name=commit.committer_contributor_name,
                            committer_contributor_key=commit.committer_contributor_key,
                            author_date=commit.author_date,
                            author_date_tz_offset=commit.author_date_tz_offset,
                            author_contributor_name=commit.author_contributor_name,
                            author_contributor_key=commit.author_contributor_key,
                            commit_message=commit.commit_message,
                            created_on_branch=commit.created_on_branch,
                            parents=commit.parents,
                            stats=commit.stats,
                            created_at=commit.created_at
                        )
                    )
                    repo_display_id_commits[display_id] = display_id_commits
                    resolved_display_id_commits_by_repo[repo] = repo_display_id_commits


map_display_ids_to_commits_page_size = 1000


def map_display_ids_to_commits(session, work_item_summaries, work_items_source_summary):
    commit_query = get_commits_query(work_items_source_summary['commit_mapping_scope'])
    earliest_created = reduce(
        lambda earliest, work_item: min(earliest, work_item['created_at']),
        work_item_summaries,
        work_item_summaries[0]['created_at']
    )
    # first get a total so we can paginate through commits
    total = session.connection.execute(
        select([func.count()]).select_from(
            commit_query.alias('T')
        ), dict(
            commit_mapping_scope_key=work_items_source_summary['commit_mapping_scope_key'],
            earliest_created=earliest_created
        )
    ).scalar()

    input_display_ids = {work_item['display_id'] for work_item in work_item_summaries}
    resolved_display_id_commits_by_repo = {}

    fetched = 0
    batch_size = map_display_ids_to_commits_page_size
    offset = 0
    while fetched < total:
        commits__batch = session.connection.execute(
            commit_query.limit(batch_size).offset(offset),
            dict(
                commit_mapping_scope_key=work_items_source_summary['commit_mapping_scope_key'],
                earliest_created=earliest_created
            )
        ).fetchall()

        resolve_display_id_commits_by_repo(
            commits__batch,
            work_items_source_summary['integration_type'],
            input_display_ids,
            resolved_display_id_commits_by_repo
        )
        offset = offset + batch_size
        fetched = fetched + len(commits__batch)

    return resolved_display_id_commits_by_repo


def resolve_commits_for_work_items(session, organization_key, work_items_source_summary, work_item_summaries):
    logger.info(
        f"Resolve commits_work_items for {organization_key} and work items source {work_items_source_summary.get('name')}")

    result = []
    if len(work_item_summaries) > 0:

        display_id_commits_by_repo = map_display_ids_to_commits(session, work_item_summaries, work_items_source_summary)
        # now map the display_id_commits mapping back out in terms of work_item_keys and commits keys
        for repo, display_id_commits in display_id_commits_by_repo.items():
            work_item_commits = []
            commits_work_items_map = {}
            for work_item in work_item_summaries:
                display_id = work_item['display_id']
                if display_id in display_id_commits:
                    resolved_commits = display_id_commits[display_id]
                    if len(resolved_commits) > 0:
                        # we found some commits referencing this work_item
                        work_item_commits.append(
                            dict(
                                work_item_key=work_item['work_item_key'],
                                commit_headers=resolved_commits
                            )
                        )
                        for commit in resolved_commits:
                            commit_key = commit['source_commit_id']
                            current_work_items = commits_work_items_map.get(commit_key, [])
                            current_work_items.append(work_item)
                            commits_work_items_map[commit_key] = current_work_items

            commits_work_items = [
                dict(
                    commit_key=commit_key,
                    work_items=work_item_summaries
                )
                for commit_key, work_item_summaries in commits_work_items_map.items()
            ]
            result.append(
                dict(
                    organization_key=organization_key,
                    repository_name=repo,
                    work_items_commits=work_item_commits,
                    commits_work_items=commits_work_items
                )
            )

        return dict(
            success=True,
            resolved=result
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
