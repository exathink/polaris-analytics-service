# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func, and_, Column, cast, Integer, or_, literal
from sqlalchemy.dialects.postgresql import UUID, insert, JSONB

from polaris.analytics.db.model import Repository, source_files, work_items, commits, \
    work_items_commits as work_items_commits_table, work_item_delivery_cycles, \
    work_item_source_file_changes
from polaris.common import db


def create_source_files(session, repository, commit_details):
    source_files_temp = db.temp_table_from(
        source_files,
        table_name='source_files_temp',
        exclude_columns=[
            source_files.c.id
        ]
    )

    source_files_temp.create(session.connection(), checkfirst=True)
    session.connection().execute(
        insert(source_files_temp).values([
            dict(
                repository_id=repository.id,
                key=source_file['key'],
                name=source_file['name'],
                path=source_file['path'],
                file_type=source_file['file_type'],
                version_count=source_file['version_count']
            )
            for commit_detail in commit_details
            for source_file in commit_detail['source_files']
        ])
    )
    # We need to do this rigmarole because the same file may be present in multiple commits, but with different
    # version counts. We cannot upsert with update on the same row twice in the same transaction. So we group
    # by source_file_key picking the maximum among all the version numbers seen in this batch.

    upsert = insert(source_files).from_select(
        ['key', 'repository_id', 'name', 'path', 'file_type', 'version_count'],
        select([
            source_files_temp.c.key,
            func.min(source_files_temp.c.repository_id).label('repository_id'),
            func.min(source_files_temp.c.name).label('name'),
            func.min(source_files_temp.c.path).label('path'),
            func.min(source_files_temp.c.file_type).label('file_type'),
            func.max(source_files_temp.c.version_count).label('version_count')
        ]).select_from(
            source_files_temp
        ).where(
            source_files_temp.c.key != None
        ).group_by(
            source_files_temp.c.key
        )
    )
    new_files = session.connection().execute(
        upsert.on_conflict_do_update(
            index_elements=['key'],
            set_=dict(
                # we do a max of the proposed insertion value and the current value to ensure
                # that the version count in monotonically increasing under updates in arbitrary order.
                version_count=func.greatest(
                    source_files.c.version_count,
                    upsert.excluded.version_count
                )
            )
        )
    ).rowcount

    return new_files


def register_source_file_versions(session, repository_key, commit_details):
    repository = Repository.find_by_repository_key(session, repository_key)
    if repository:
        new_files = create_source_files(session, repository, commit_details)

        return dict(
            new_file_count=new_files
        )


def populate_work_item_source_file_changes(session, commits_temp):
    updated = 0

    source_files_json = select([
        commits.c.id.label('commit_id'),
        func.jsonb_array_elements(commits.c.source_files).label('source_file')
    ]).select_from(
        commits_temp.join(
            commits, commits.c.key == commits_temp.c.commit_key
        )
    ).alias('source_files_json')

    source_file_changes = select([
        source_files_json.c.commit_id,
        source_files.c.id.label('source_file_id'),
        source_files.c.repository_id.label('repository_id'),
        cast(source_files_json.c.source_file, JSONB)['action'].label('file_action'),
        cast(cast(source_files_json.c.source_file, JSONB)['stats']['lines'].astext, Integer).label(
            'total_lines_changed'),
        cast(cast(source_files_json.c.source_file, JSONB)['stats']['deletions'].astext, Integer).label(
            'total_lines_deleted'),
        cast(cast(source_files_json.c.source_file, JSONB)['stats']['insertions'].astext, Integer).label(
            'total_lines_added')
    ]).select_from(
        source_files_json.join(
            source_files, cast(cast(source_files_json.c.source_file, JSONB)['key'].astext, UUID) == source_files.c.key
        )
    ).alias('source_file_changes')

    # Create a temp table to store data for commits within delivery cycles
    work_item_source_file_changes_temp = db.temp_table_from(
        work_item_source_file_changes,
        table_name='work_item_source_file_changes_temp',
        exclude_columns=[
            work_item_source_file_changes.c.id
        ]
    )
    work_item_source_file_changes_temp.create(session.connection(), checkfirst=True)

    # select relevant rows and insert into temp table
    session.connection().execute(
        work_item_source_file_changes_temp.insert().from_select([
            'commit_id',
            'work_item_id',
            'delivery_cycle_id',
            'repository_id',
            'source_file_id',
            'source_commit_id',
            'commit_date',
            'committer_contributor_alias_id',
            'author_contributor_alias_id',
            'num_parents',
            'created_on_branch',
            'file_action',
            'total_lines_changed',
            'total_lines_deleted',
            'total_lines_added'
        ],
            select([
                commits.c.id.label('commit_id'),
                work_items.c.id.label('work_item_id'),
                work_item_delivery_cycles.c.delivery_cycle_id.label('delivery_cycle_id'),
                source_file_changes.c.repository_id.label('repository_id'),
                source_file_changes.c.source_file_id.label('source_file_id'),
                commits.c.source_commit_id,
                commits.c.commit_date,
                commits.c.committer_contributor_alias_id,
                commits.c.author_contributor_alias_id,
                commits.c.num_parents,
                commits.c.created_on_branch,
                source_file_changes.c.file_action.label('file_action'),
                source_file_changes.c.total_lines_changed.label('total_lines_changed'),
                source_file_changes.c.total_lines_deleted.label('total_lines_deleted'),
                source_file_changes.c.total_lines_added.label('total_lines_added')
            ]).select_from(
                commits_temp.join(
                    commits, commits_temp.c.commit_key == commits.c.key
                ).join(
                    work_items_commits_table, commits.c.id == work_items_commits_table.c.commit_id
                ).join(
                    work_items, work_items.c.id == work_items_commits_table.c.work_item_id
                ).join(
                    work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
                ).join(
                    source_file_changes, source_file_changes.c.commit_id == commits.c.id
                )
            ).where(
                and_(
                    commits.c.commit_date >= work_item_delivery_cycles.c.start_date,
                    or_(
                        work_item_delivery_cycles.c.end_date == None,
                        commits.c.commit_date <= work_item_delivery_cycles.c.end_date
                    )
                )
            )
        )
    )

    # Find commits outside all delivery cycles
    session.connection().execute(
        work_item_source_file_changes_temp.insert().from_select([
            'commit_id',
            'work_item_id',
            'delivery_cycle_id',
            'repository_id',
            'source_file_id',
            'source_commit_id',
            'commit_date',
            'committer_contributor_alias_id',
            'author_contributor_alias_id',
            'num_parents',
            'created_on_branch',
            'file_action',
            'total_lines_changed',
            'total_lines_deleted',
            'total_lines_added'
        ],
            select([
                commits.c.id.label('commit_id'),
                work_items.c.id.label('work_item_id'),
                literal(None).label('delivery_cycle_id'),
                source_file_changes.c.repository_id.label('repository_id'),
                source_file_changes.c.source_file_id.label('source_file_id'),
                commits.c.source_commit_id,
                commits.c.commit_date,
                commits.c.committer_contributor_alias_id,
                commits.c.author_contributor_alias_id,
                commits.c.num_parents,
                commits.c.created_on_branch,
                source_file_changes.c.file_action.label('file_action'),
                source_file_changes.c.total_lines_changed.label('total_lines_changed'),
                source_file_changes.c.total_lines_deleted.label('total_lines_deleted'),
                source_file_changes.c.total_lines_added.label('total_lines_added')
            ]).select_from(
                commits_temp.join(
                    commits, commits_temp.c.commit_key == commits.c.key
                ).join(
                    work_items_commits_table, commits.c.id == work_items_commits_table.c.commit_id
                ).join(
                    work_items, work_items.c.id == work_items_commits_table.c.work_item_id
                ).outerjoin(
                    work_item_source_file_changes_temp,
                    and_(
                        commits.c.id == work_item_source_file_changes_temp.c.commit_id,
                        work_items.c.id == work_item_source_file_changes_temp.c.work_item_id
                    )
                ).join(
                    source_file_changes, source_file_changes.c.commit_id == commits.c.id
                )
            ).where(
                work_item_source_file_changes_temp.c.commit_id == None
            )
        )
    )

    upsert_stmt = insert(work_item_source_file_changes).from_select(
        [
            'commit_id',
            'work_item_id',
            'delivery_cycle_id',
            'repository_id',
            'source_file_id',
            'source_commit_id',
            'commit_date',
            'committer_contributor_alias_id',
            'author_contributor_alias_id',
            'num_parents',
            'created_on_branch',
            'file_action',
            'total_lines_changed',
            'total_lines_deleted',
            'total_lines_added'

        ],
        select([
            work_item_source_file_changes_temp.c.commit_id,
            work_item_source_file_changes_temp.c.work_item_id,
            work_item_source_file_changes_temp.c.delivery_cycle_id,
            work_item_source_file_changes_temp.c.repository_id,
            work_item_source_file_changes_temp.c.source_file_id.label('source_file_id'),
            work_item_source_file_changes_temp.c.source_commit_id,
            work_item_source_file_changes_temp.c.commit_date,
            work_item_source_file_changes_temp.c.committer_contributor_alias_id,
            work_item_source_file_changes_temp.c.author_contributor_alias_id,
            work_item_source_file_changes_temp.c.num_parents,
            work_item_source_file_changes_temp.c.created_on_branch,
            work_item_source_file_changes_temp.c.file_action,
            work_item_source_file_changes_temp.c.total_lines_changed,
            work_item_source_file_changes_temp.c.total_lines_deleted,
            work_item_source_file_changes_temp.c.total_lines_added
        ]
        ).select_from(
            work_item_source_file_changes_temp
        )
    )

    updated = session.connection().execute(
        upsert_stmt.on_conflict_do_update(
            index_elements=['commit_id', 'work_item_id', 'delivery_cycle_id', 'repository_id', 'source_file_id'],
            set_=dict(
                source_commit_id=upsert_stmt.excluded.source_commit_id,
                commit_date=upsert_stmt.excluded.commit_date,
                committer_contributor_alias_id=upsert_stmt.excluded.committer_contributor_alias_id,
                author_contributor_alias_id=upsert_stmt.excluded.author_contributor_alias_id,
                num_parents=upsert_stmt.excluded.num_parents,
                created_on_branch=upsert_stmt.excluded.created_on_branch,
                file_action=upsert_stmt.excluded.file_action,
                total_lines_changed=upsert_stmt.excluded.total_lines_changed,
                total_lines_deleted=upsert_stmt.excluded.total_lines_deleted,
                total_lines_added=upsert_stmt.excluded.total_lines_added
            )
        )
    ).rowcount
    return dict(
        updated=updated
    )


def populate_work_item_source_file_changes_for_commits(session, commit_details):
    updated = 0
    if len(commit_details) > 0:
        # create a temp table for received commit ids
        commits_temp = db.create_temp_table(
            table_name='commits_temp',
            columns=[
                Column('commit_key', UUID(as_uuid=True)),
            ]
        )
        commits_temp.create(session.connection(), checkfirst=True)

        # Get distinct commit keys from input
        distinct_commits = set()
        for entry in commit_details:
            distinct_commits.add(entry['key'])

        session.connection().execute(
            commits_temp.insert().values(
                [
                    dict(
                        commit_key=record
                    )
                    for record in distinct_commits
                ]
            )
        )
        result = populate_work_item_source_file_changes(session, commits_temp)
        updated = result['updated']
    return dict(
        updated=updated
    )


def populate_work_item_source_file_changes_for_work_items(session, work_items_commits):
    updated = 0
    if len(work_items_commits) > 0:
        # create a temp table for commit ids from work_items_commits
        commits_temp = db.create_temp_table(
            table_name='commits_temp',
            columns=[
                Column('commit_key', UUID(as_uuid=True)),
            ]
        )
        commits_temp.create(session.connection(), checkfirst=True)

        # Get distinct commit keys from input
        distinct_commits = set()
        for entry in work_items_commits:
            distinct_commits.add(entry['commit_key'])

        session.connection().execute(
            commits_temp.insert().values(
                [
                    dict(
                        commit_key=record
                    )
                    for record in distinct_commits
                ]
            )
        )
        result = populate_work_item_source_file_changes(session, commits_temp)
        updated = result['updated']

    return dict(
        updated=updated
    )
