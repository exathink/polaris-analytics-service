# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from sqlalchemy import Column, BigInteger, Integer, Boolean, Text, String, UniqueConstraint, ForeignKey, Index, DateTime
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
from sqlalchemy.orm import relationship

from polaris.common import db

Base = db.polaris_declarative_base(schema='analytics')


class Contributor(Base):
    __tablename__ = 'contributors'

    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True, nullable=False)
    name = Column(String, nullable=False)

    aliases = relationship('ContributorAlias', back_populates='contributor')

contributors = Contributor.__table__



class ContributorAlias(Base):
    __tablename__ = 'contributor_aliases'
    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True, nullable=False)
    name = Column(String, nullable=False)
    source = Column(String, nullable=False)
    source_alias = Column(String, nullable=False)

    contributor_id = Column(Integer, ForeignKey('contributors.id'), index=True, nullable=False)
    contributor = relationship('Contributor', back_populates='aliases')

contributor_aliases = ContributorAlias.__table__


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
    committer_contributor_alias_id = Column(Integer, ForeignKey('contributor_aliases.id'), nullable=False, index=True)


    author_contributor_name = Column(String, nullable=True)
    author_contributor_key = Column(UUID(as_uuid=True), nullable=True)
    author_date = Column(DateTime, nullable=True)
    author_date_tz_offset = Column(Integer, default=0)
    author_contributor_alias_id = Column(Integer, ForeignKey('contributor_aliases.id'), nullable=False, index=True)





    is_orphan = Column(Boolean, default=False)
    created_at = Column(DateTime, nullable=True)
    created_on_branch = Column(String, nullable=True)

    num_parents = Column(Integer, default=1)
    parents = Column(ARRAY(String), nullable=True)
    stats  = Column(JSONB, nullable=True)

    # relationships
    committer_alias = relationship('ContributorAlias', foreign_keys=[committer_contributor_alias_id])
    author_alias = relationship('ContributorAlias', foreign_keys=[author_contributor_alias_id])


commits = Commit.__table__

UniqueConstraint(commits.c.repository_key, commits.c.source_commit_id)
Index('ix_analytics_commits_author_committer_contributor_key', commits.c.author_contributor_key, commits.c.committer_contributor_key)





def recreate_all(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)