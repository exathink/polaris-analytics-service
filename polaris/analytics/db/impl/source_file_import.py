# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert

from polaris.analytics.db.model import Repository, source_files
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

def populate_work_item_source_file_changes_for_commits(session, repository_key, commit_details):
    updated = 0
    return dict(
        updated=updated
    )
