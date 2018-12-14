# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.common import db

from sqlalchemy import MetaData, Table, Column,BigInteger,  ForeignKeyConstraint, Integer, Boolean, Text, String, UniqueConstraint,  ForeignKey, Index, DateTime
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
from sqlalchemy.orm import relationship

Base = db.polaris_declarative_base(schema='analytics')

# many-many relationship table
commits_work_items = Table(
    'commits_work_items', Base.metadata,
    Column('commit_id', ForeignKey('commits.id'), primary_key=True, index=True),
    Column('work_item_id', ForeignKey('work_items.id'), primary_key=True, index=True)
)

class Contributor(Base):
    __tablename__ = 'contributors'

    id = Column(Integer, primary_key=True)
    key=Column(UUID(as_uuid=True), unique=True, nullable=False)
    name=Column(String, nullable=False)
    source=Column(String, nullable=False)
    source_alias=Column(String, nullable=False)
    alias_for=Column(UUID(as_uuid=True), ForeignKey('contributors.key'), nullable=True)




contributors = Contributor.__table__
Index('ix_contributors_key_alias_for', contributors.c.key, contributors.c.alias_for),

class Commit(Base):
    __tablename__ = 'commits'

    id = Column(BigInteger, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True, index=True, nullable=False)
    organization_key = Column(UUID(as_uuid=True), index=True, nullable=False)
    repository_key = Column(UUID(as_uuid=True), index=True, nullable=False)

    # This is the id of the commit within the repository. For git it is the commit hash.
    source_commit_id = Column(String, nullable=False)

    commit_message = Column(Text, default='')

    committer_contributor_name = Column(String, nullable=True)
    committer_contributor_key = Column(UUID(as_uuid=True), nullable=True)
    commit_date = Column(DateTime, index=True, nullable=False)
    commit_date_tz_offset = Column(Integer, default=0)
    committer_contributor_id = Column(Integer, ForeignKey('contributors.id'), nullable=False, index=True)


    author_contributor_name = Column(String, nullable=True)
    author_contributor_key = Column(UUID(as_uuid=True), nullable=True)
    author_date = Column(DateTime, nullable=True)
    author_date_tz_offset = Column(Integer, default=0)
    author_contributor_id = Column(Integer, ForeignKey('contributors.id'), nullable=False, index=True)





    is_orphan = Column(Boolean, default=False)
    created_at = Column(DateTime, nullable=True)
    created_on_branch = Column(String, nullable=True)

    num_parents = Column(Integer, default=1)
    parents = Column(ARRAY(String), nullable=True)
    stats  = Column(JSONB, nullable=True)

    # relationships
    committer = relationship('Contributor', foreign_keys=['committer_contributor_id'])
    author = relationship('Contributor', foreign_keys=['author_contributor_id'])
    work_items = relationship('WorkItem',
                              secondary=commits_work_items,
                              back_populates="commits")

commits = Commit.__table__

UniqueConstraint(commits.c.repository_key, commits.c.source_commit_id)
Index('ix_analytics_commits_author_contributor_key', commits.c.author_contributor_key, commits.c.committer_contributor_key)

class WorkItemsSource(Base):
    __tablename__ = 'work_items_sources'

    id = Column(Integer, primary_key=True)
    key =  Column(UUID(as_uuid=True), nullable=False, unique=True)
    # type of integration: github, github_enterprise, jira, pivotal_tracker etc..
    integration_type = Column(String, nullable=False)

    # User facing display name for the instance.
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    # An instance must be tied to an account and an organization
    account_key = Column(UUID(as_uuid=True), nullable=False)
    organization_key = Column(UUID(as_uuid=True), nullable=False)
    # Commit mapping scope specifies the repositories that are mapped to this
    # work item source. The valid values are ('organization', 'project', 'repository')
    # Given the commit mapping scope key, commits originating from all repositories
    # within that specific scope (instance of org, project or repository) will be evaluated to
    # see if they can be mapped to a given work item originating from this work items source.
    commit_mapping_scope = Column(String, nullable=False, default='organization', server_default="'organization'")
    commit_mapping_scope_key = Column(UUID(as_uuid=True), nullable=False)

work_items_sources = WorkItemsSource.__table__


class WorkItem(Base):
    __tablename__ = 'work_items'

    id = Column(BigInteger, primary_key=True)
    key = Column(UUID(as_uuid=True), nullable=False, unique=True)
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    is_bug = Column(Boolean, nullable=False, default=False, server_default='FALSE')
    tags = Column(ARRAY(String), nullable=False, default=[], server_default='{}')

    url=Column(String, nullable=True)
    created_at=Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=True)
    display_id = Column(String, nullable=False)
    state=Column(String, nullable=False)

    # Work Items Source relationship
    work_items_source_id = Column(Integer, ForeignKey('work_items_sources.id'))
    work_items_source = relationship('WorkItemsSource', back_populates='work_items')
    commits = relationship("Commit",
                                 secondary=commits_work_items,
                                 back_populates="work_items")



def recreate_all(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)