# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



from sqlalchemy import MetaData, Table, Column,BigInteger,  ForeignKeyConstraint, Integer, Boolean, Text, String, UniqueConstraint,  ForeignKey, Index, DateTime
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB


analytics = MetaData(schema='analytics')

contributors = Table(
    'contributors', analytics,
    Column('id', Integer, primary_key=True),
    Column('key', UUID(as_uuid=True), unique=True, nullable=False),
    Column('name', String, nullable=False),
    Column('source', String, nullable=False),
    Column('alias_for', UUID(as_uuid=True), ForeignKey('contributors.key'), nullable=True ),

    Index('ix_contributors_key_alias_for', 'key', 'alias_for'),
    UniqueConstraint('source', 'key')
)







commits = Table(
    'commits', analytics,
    Column('id', BigInteger, primary_key=True),
    Column('key', UUID(as_uuid=True), unique=True, nullable=False),
    Column('repository_key', UUID(as_uuid=True),  index=True, nullable=False),
    Column('source_id', String, nullable=False),

    Column('commit_message', Text, default=''),

    Column('committer_contributor_name', String, nullable=True),
    Column('committer_contributor_key', UUID(as_uuid=True), nullable=True),
    Column('commit_date', DateTime, index=True, nullable=False),
    Column('commit_date_tz_offset', Integer, default=0),

    Column('author_contributor_name', String, nullable=True),
    Column('author_contributor_key', UUID(as_uuid=True), nullable=True),
    Column('author_date', DateTime, nullable=True),
    Column('author_date_tz_offset', Integer, default=0),


    Column('parents', ARRAY(String)),

    Column('is_orphan', Boolean, default=False),
    Column('created_at', DateTime, nullable=True),
    Column('created_on_branch', String, nullable=True),

    Column('num_parents', Integer, default=1),
    Column('stats', JSONB, nullable=False),
    UniqueConstraint('repository_key', 'source_id'),
    Index('ix_analytics_commits_author_contributor_key', 'author_contributor_key', 'committer_contributor_key')


)

work_items_sources = Table(
    'work_items_sources', analytics,
    Column('id', Integer, primary_key=True),
    Column('key',UUID(as_uuid=True), nullable=False, unique=True),
    # type of integration: github, github_enterprise, jira, pivotal_tracker etc..
    Column('integration_type', String, nullable=False),
    # User facing display name for the instance.
    Column('name', String, nullable=False),
    Column('description', Text, nullable=True),

    Column('account_key', UUID(as_uuid=True), nullable=False),
    Column('organization_key', UUID(as_uuid=True), nullable=False),
    # Commit mapping scope specifies the repositories that are mapped to this
    # work item source. The valid values are ('organization', 'project', 'repository')
    # Given the commit mapping scope key, commits originating from all repositories
    # within that specific scope (instance of org, project or repository) will be evaluated to
    # see if they can be mapped to a given work item originating from this work items source.
    Column('commit_mapping_scope', String, nullable=False, default='organization', server_default="'organization'"),
    Column('commit_mapping_scope_key',UUID(as_uuid=True), nullable=False)

)

work_items = Table(
    'work_items', analytics,
    Column('id', BigInteger, primary_key=True),
    Column('key', UUID(as_uuid=True), nullable=False, unique=True),
    Column('name', String(256), nullable=False),
    Column('description', Text, nullable=True),
    Column('is_bug', Boolean, nullable=False, default=False, server_default='FALSE'),
    Column('tags', ARRAY(String), nullable=False, default=[], server_default='{}'),
    Column('url', String, nullable=True),
    Column('created_at', DateTime, nullable=False),
    Column('updated_at', DateTime, nullable=True),
    Column('display_id', String, nullable=False),
    Column('state', String, nullable=False),
    Column('work_items_source_id', Integer, ForeignKey('work_items_sources.id'), nullable=False)
)

commits_work_items = Table(
    'commits_work_items', analytics,
    Column('commit_id', BigInteger, ForeignKey('commits.id'), index=True, nullable=False),
    Column('work_item_id', BigInteger, ForeignKey('work_items.id'), index=True, nullable=False),
    UniqueConstraint('commit_id', 'work_item_id')
)

