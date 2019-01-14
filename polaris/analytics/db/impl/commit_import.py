# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.common import db
from polaris.utils.collections import dict_select, dict_summarize_totals
from polaris.analytics.db.model import commits, contributors, contributor_aliases, Repository, repositories, \
    repositories_contributor_aliases

from sqlalchemy import Column, String, select, Integer, and_, bindparam, func, distinct, or_, case

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
                    key=contributor_alias['key'],
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
                    key=contributor_alias['key'],
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
            Column('commit_id', Integer, nullable=True),

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

    repository = session.connection.execute(
        select([repositories.c.id, repositories.c.commit_count, repositories.c.earliest_commit,
                repositories.c.latest_commit]).where(
            repositories.c.key == bindparam('repository_key')
        ),
        dict(repository_key=repository_key)
    ).fetchone()

    session.connection.execute(
        commits_temp.insert([
            dict(
                repository_id=repository.id,
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

    # mark existing_commits
    session.connection.execute(
        commits_temp.update().values(
            commit_id=select(
                [commits.c.id.label('commit_id')]
            ).where(
                and_(
                    commits_temp.c.key == commits.c.key
                )
            )
        )
    )
    # insert new commits
    commit_columns = [
        column
        for column in commits_temp.columns
        if column.name not in ['commit_id', 'committer_alias_key', 'author_alias_key']
    ]

    session.connection.execute(
        insert(commits).from_select(
            [column.name for column in commit_columns],
            select(commit_columns).where(
                commits_temp.c.commit_id == None
            )
        )
    )

    # update repository stats
    update_repository_stats(session, repository, commits_temp)
    # update contributor_alias_stats

    update_repositories_contributor_aliases(session, repository, commits_temp)

    new_commits = session.connection.execute(
        select(commit_columns).where(
            commits_temp.c.commit_id == None
        )
    ).fetchall()

    return dict(
        new_commits=db.row_proxies_to_dict(new_commits),
        new_contributors=new_contributors
    )


def update_repositories_contributor_aliases(session, repository, commits_temp, ):
    # we have marked all existing commit id in an earlier stage and stored in
    # in commit summary temp. Now we use this table to group the new commits
    # by contributor alias and insert contributor_aliases with their stats
    # into the repository_contributor_aliases table. Note we are denormalzing
    # serveral fields from contributor_aliases and contributors onto this
    # relationship table so that we can speed up aggregates for contributors.

    to_upsert = select([
        bindparam('repository_id').label('repository_id'),
        contributor_aliases.c.id.label('contributor_alias_id'),
        contributor_aliases.c.contributor_id.label('contributor_id'),
        contributor_aliases.c.robot.label('robot'),
        func.count(distinct(commits_temp.c.source_commit_id)).label('commit_count'),
        func.min(commits_temp.c.commit_date).label('earliest_commit'),
        func.max(commits_temp.c.commit_date).label('latest_commit')
    ]).select_from(
        commits_temp.join(
            contributor_aliases,
            or_(
                commits_temp.c.author_contributor_alias_id == contributor_aliases.c.id,
                commits_temp.c.committer_contributor_alias_id == contributor_aliases.c.id
            )
        )
    ).where(
        commits_temp.c.commit_id == None
    ).group_by(contributor_aliases.c.id)

    # We can limit this to only new commits under the inductive assumption
    # that existing commits and their stats are reflected in the current state
    # of the table. Only new commits can give rise to new contributor aliases for the repo.
    # If an new commit from a new alias is seen it is inserted, and if a new commit from
    # and existing alias is seen it is updated via the upsert statement.
    upsert = insert(repositories_contributor_aliases).from_select(
        [
            to_upsert.c.repository_id,
            to_upsert.c.contributor_alias_id,
            to_upsert.c.contributor_id,
            to_upsert.c.robot,
            to_upsert.c.commit_count,
            to_upsert.c.earliest_commit,
            to_upsert.c.latest_commit
        ],
        to_upsert
    )

    session.connection.execute(
        upsert.on_conflict_do_update(
            index_elements=['repository_id', 'contributor_alias_id'],
            set_=dict(
                contributor_id=upsert.excluded.contributor_id,
                robot=upsert.excluded.robot,
                commit_count=upsert.excluded.commit_count + repositories_contributor_aliases.c.commit_count,
                earliest_commit=case(
                    [(upsert.excluded.earliest_commit < repositories_contributor_aliases.c.earliest_commit,
                      upsert.excluded.earliest_commit)],
                    else_=repositories_contributor_aliases.c.earliest_commit
                ),
                latest_commit=case(
                    [(upsert.excluded.latest_commit > repositories_contributor_aliases.c.latest_commit,
                      upsert.excluded.latest_commit)],
                    else_=repositories_contributor_aliases.c.latest_commit
                )
            )
        ), dict(repository_id=repository.id)
    )


def update_repository_stats(session, repository, commits_temp):
    new_commits_stats = session.connection.execute(
        select(
            [
                func.min(commits_temp.c.commit_date).label('earliest_commit'),
                func.max(commits_temp.c.commit_date).label('latest_commit'),
                func.count(commits_temp.c.key).label('commit_count')
            ]
        ).where(
            commits_temp.c.commit_id == None
        )
    ).fetchone()

    if new_commits_stats.commit_count > 0:
        updated_columns = {}
        if repository['earliest_commit'] is None or new_commits_stats.earliest_commit < repository['earliest_commit']:
            updated_columns['earliest_commit'] = new_commits_stats.earliest_commit
        if repository['latest_commit'] is None or new_commits_stats.latest_commit > repository['latest_commit']:
            updated_columns['latest_commit'] = new_commits_stats.latest_commit

        current_commits = repository['commit_count'] if repository['commit_count'] is not None else 0
        updated_columns['commit_count'] = current_commits + new_commits_stats.commit_count

        if len(updated_columns) > 0:
            session.connection.execute(
                repositories.update().where(
                    repositories.c.id == repository.id
                ).values(updated_columns)
            )


def import_commit_details(session, repository_key, commit_details):
    repository = Repository.find_by_repository_key(session, repository_key)
    if repository:
        commits_temp = db.create_temp_table('commits_temp', [
            commits.c.key,
            commits.c.stats,
            commits.c.parents,
            commits.c.num_parents,
            commits.c.source_files,
            commits.c.source_file_types_summary,
            commits.c.source_file_actions_summary,
        ])
        commits_temp.create(session.connection(), checkfirst=True)

        session.connection().execute(
            commits_temp.insert().values([
                dict(
                    key=commit_detail['key'],
                    parents=commit_detail['parents'],
                    stats=commit_detail['stats'],
                    num_parents=len(commit_detail['parents']),
                    source_files=commit_detail['source_files'],
                    source_file_types_summary=dict_summarize_totals(commit_detail['source_files'], field='file_type'),
                    source_file_actions_summary=dict_summarize_totals(commit_detail['source_files'], field='action')
                )
                for commit_detail in commit_details
            ])
        )

        update_count = session.connection().execute(
            commits.update().where(
                commits.c.key == commits_temp.c.key
            ).values(
                parents=commits_temp.c.parents,
                stats=commits_temp.c.stats,
                num_parents=commits_temp.c.num_parents,
                source_files=commits_temp.c.source_files,
                source_file_types_summary=commits_temp.c.source_file_types_summary,
                source_file_actions_summary=commits_temp.c.source_file_actions_summary
            )
        ).rowcount

        return dict(
            update_count=update_count
        )


