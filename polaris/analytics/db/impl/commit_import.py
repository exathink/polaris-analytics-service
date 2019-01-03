# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from polaris.common import db
from polaris.utils.collections import dict_select
from polaris.analytics.db.model import commits, contributors, contributor_aliases, repositories
from sqlalchemy import Column, String, select, BigInteger, Integer, and_, bindparam

from sqlalchemy.dialects.postgresql import insert, UUID

def import_new_contributor_aliases(session, new_contributor_aliases):
    if len(new_contributor_aliases) > 0:
        ca_temp = db.temp_table_from(
            contributor_aliases,
            table_name='ca_temp',
            exclude_columns=[contributor_aliases.c.id, contributor_aliases.c.contributor_id]
        )
        ca_temp.create(session.connection, checkfirst=True)

        session.connection.execute(
            insert(ca_temp).values([
                dict(
                    key=contributor_alias['contributor_key'],
                    name=contributor_alias['name'],
                    source='vcs',
                    source_alias=contributor_alias['alias']
                )
                for contributor_alias in new_contributor_aliases
            ])
        )
        # Now insert a new contributor record for the new alias with same key and contributor alias
        # Ignore if duplicate.
        session.connection.execute(
            insert(contributors).values([
                dict(
                    key=contributor_alias['contributor_key'],
                    name=contributor_alias['name'],
                )
                for contributor_alias in new_contributor_aliases
            ]).on_conflict_do_nothing(
                index_elements=['key']
            )
        )
        # Now insert contributor_alias records with contributor_id resolved.
        session.connection.execute(
            insert(contributor_aliases).from_select(
                [
                    'key', 'name', 'source', 'source_alias', 'contributor_id'
                ],
                select([
                    ca_temp.c.key,
                    ca_temp.c.name,
                    ca_temp.c.source,
                    ca_temp.c.source_alias,
                    contributors.c.id.label('contributor_id')
                ]).select_from(
                    ca_temp.join(contributors, contributors.c.key == ca_temp.c.key)
                )
            ).on_conflict_do_nothing(
                index_elements=['key']
            )
        )



def import_new_commits(session, organization_key, repository_key, new_commits, new_contributors):

    import_new_contributor_aliases(session, new_contributors)

    commits_temp = db.temp_table_from(
        commits,
        table_name='commits_temp',
        exclude_columns=[
            commits.c.id,

            commits.c.committer_contributor_alias_id,
            commits.c.committer_contributor_name,
            commits.c.committer_contributor_key,

            commits.c.author_contributor_alias_id,
            commits.c.author_contributor_name,
            commits.c.author_contributor_key,
        ],
        extra_columns=[

            Column('committer_alias_key', UUID, nullable=False),
            Column('committer_contributor_alias_id', Integer, nullable=True),
            Column('committer_contributor_name', String, nullable=True),
            Column('committer_contributor_key', UUID, nullable=True),

            Column('author_alias_key', UUID, nullable=False),
            Column('author_contributor_alias_id', Integer, nullable=True),
            Column('author_contributor_name', String, nullable=True),
            Column('author_contributor_key', UUID, nullable=True),

        ]
    )
    commits_temp.create(session.connection, checkfirst=True)

    repository_id = session.connection.execute(
        select([repositories.c.id]).where(
            repositories.c.key == bindparam('repository_key')
        ),
        dict(repository_key=repository_key)
    ).scalar()


    session.connection.execute(
        commits_temp.insert([
            dict(
                repository_id=repository_id,
                source_commit_id=commit['source_commit_id'],
                **dict_select(
                    commit, [
                        'key',
                        'commit_date',
                        'commit_date_tz_offset',
                        'committer_alias_key',
                        'author_date',
                        'author_date_tz_offset',
                        'author_alias_key',
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
            dict(
                committer_contributor_alias_id=contributor_aliases.c.id,
                committer_contributor_key=contributors.c.key,
                committer_contributor_name=contributors.c.name,
            )
        ).where(
            and_(
                contributor_aliases.c.key == commits_temp.c.committer_alias_key,
                contributors.c.id == contributor_aliases.c.contributor_id
            )
        )
    )

    # resolve author_keys
    session.connection.execute(
        commits_temp.update().values(
            dict(
                author_contributor_alias_id=contributor_aliases.c.id,
                author_contributor_key=contributors.c.key,
                author_contributor_name=contributors.c.name,
            )
        ).where(
            and_(
                contributor_aliases.c.key == commits_temp.c.author_alias_key,
                contributors.c.id == contributor_aliases.c.contributor_id
            )
        )
    )

    commit_columns = [
        column
        for column in commits_temp.columns
        if column.name not in ['committer_alias_key', 'author_alias_key']
    ]
    session.connection.execute(
        insert(commits).from_select(
            [column.name for column in commit_columns],
            select(commit_columns)
        ).on_conflict_do_nothing(
            index_elements=['repository_id', 'source_commit_id']
        )
    )

    new_commits  = session.connection.execute(
        select(
            [
                *commits.columns
            ]
        ).select_from(
            commits_temp.join(
                commits,
                and_(
                    commits_temp.c.repository_id == commits.c.repository_id,
                    commits_temp.c.source_commit_id == commits.c.source_commit_id
                )
            )
        )
    ).fetchall()

    return dict(
        new_commits = db.row_proxies_to_dict(new_commits),
        new_contributors = new_contributors
    )



def import_commit_details(session, commit_details):
    commits_temp = db.create_temp_table('commits_temp', [
        commits.c.key,
        commits.c.stats,
        commits.c.parents,
        commits.c.num_parents
    ])
    commits_temp.create(session.connection, checkfirst=True)

    session.connection.execute(
        commits_temp.insert().values([
            dict(
                key=commit_detail['key'],
                parents=commit_detail['parents'],
                stats=commit_detail['stats'],
                num_parents=len(commit_detail['parents'])
            )
            for commit_detail in commit_details
        ])
    )

    update_count = session.connection.execute(
        commits.update().where(
            commits.c.key == commits_temp.c.key
        ).values(
            parents=commits_temp.c.parents,
            stats=commits_temp.c.stats,
            num_parents=commits_temp.c.num_parents
        )
    ).rowcount

    return dict(
        update_count=update_count
    )







