# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from polaris.common import db
from polaris.utils.collections import dict_select
from polaris.analytics.db.model import commits, contributors
from sqlalchemy import Column, select, BigInteger, Integer, and_

from sqlalchemy.dialects.postgresql import insert

def import_new_contributors(session, new_contributors):
    if len(new_contributors) > 0:
        session.connection.execute(
            insert(contributors).values([
                dict(
                    key=contributor['contributor_key'],
                    name=contributor['name'],
                    source='vcs',
                    source_alias=contributor['alias']
                )
                for contributor in new_contributors
            ]).on_conflict_do_nothing(
                index_elements=['key']
            )
        )

def import_new_commits(session, organization_key, repository_key, new_commits, new_contributors):

    import_new_contributors(session, new_contributors)

    commits_temp = db.temp_table_from(
        commits,
        table_name='commits_temp',
        exclude_columns=[commits.c.id, commits.c.committer_contributor_id, commits.c.author_contributor_id],
        extra_columns=[
            Column('committer_contributor_id', Integer, nullable=True),
            Column('author_contributor_id', Integer, nullable=True)
        ]
    )
    commits_temp.create(session.connection, checkfirst=True)

    session.connection.execute(
        commits_temp.insert([
            dict(
                organization_key=organization_key,
                repository_key=repository_key,
                key = uuid.uuid4().hex,
                source_commit_id=commit['source_commit_id'],
                **dict_select(
                    commit, [
                        'commit_date',
                        'commit_date_tz_offset',
                        'committer_contributor_key',
                        'committer_contributor_name',
                        'author_date',
                        'author_date_tz_offset',
                        'author_contributor_key',
                        'author_contributor_name',
                        'commit_message',
                        'created_at',
                        'created_on_branch'
                    ]
                )
            )
            for commit in new_commits
        ])
    )

    # resolve committer_keys
    session.connection.execute(
        commits_temp.update().values(
            committer_contributor_id=select([
                contributors.c.id.label('committer_contributor_id')
            ]).where(
                contributors.c.key == commits_temp.c.committer_contributor_key
            ).limit(1)
        )
    )

    # resolve author_keys
    session.connection.execute(
        commits_temp.update().values(
            author_contributor_id=select([
                contributors.c.id.label('author_contributor_id')
            ]).where(
                contributors.c.key == commits_temp.c.author_contributor_key
            ).limit(1)
        )
    )


    session.connection.execute(
        insert(commits).from_select(
            [column.name for column in commits_temp.columns],
            select([commits_temp])
        ).on_conflict_do_nothing(
            index_elements=['repository_key', 'source_commit_id']
        )
    )

    new_commits_with_keys = session.connection.execute(
        select(
            [
                commits.c.key.label('commit_key'),
                *commits.columns
            ]
        ).select_from(
            commits_temp.join(
                commits,
                and_(
                    commits_temp.c.repository_key == commits.c.repository_key,
                    commits_temp.c.source_commit_id == commits.c.source_commit_id
                )
            )
        )
    ).fetchall()

    return dict(
        new_commits = db.row_proxies_to_dict(new_commits_with_keys),
        new_contributors = new_contributors
    )



def import_commit_details(session, repository_key, commit_details):
    commits_temp = db.create_temp_table('commits_temp', [
        commits.c.repository_key,
        commits.c.source_commit_id,
        commits.c.stats,
        commits.c.parents,
        commits.c.num_parents
    ])
    commits_temp.create(session.connection, checkfirst=True)

    session.connection.execute(
        commits_temp.insert().values([
            dict(
                repository_key=repository_key,
                source_commit_id=commit_detail['source_commit_id'],
                parents=commit_detail['parents'],
                stats=commit_detail['stats'],
                num_parents=len(commit_detail['parents'])
            )
            for commit_detail in commit_details
        ])
    )

    commits_updated = session.connection.execute(
        commits.update().where(
            and_(
                commits.c.repository_key==commits_temp.c.repository_key,
                commits.c.source_commit_id==commits_temp.c.source_commit_id
            )
        ).values(
            parents=commits_temp.c.parents,
            stats=commits_temp.c.stats,
            num_parents=commits_temp.c.num_parents
        )
    ).rowcount

    return dict(
        commits_updated=commits_updated
    )







