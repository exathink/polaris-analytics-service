# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
import copy
from datetime import datetime
from logging import getLogger

from sqlalchemy import Table, Column, BigInteger, Integer, Float, Boolean, text, Text, String, UniqueConstraint, ForeignKey, \
    Index, DateTime, and_
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
from sqlalchemy.orm import relationship, object_session

from polaris.common import db
from polaris.common.enums import AccountRoles, OrganizationRoles, WorkTrackingIntegrationType
from polaris.analytics.db.enums import WorkItemsStateType
from polaris.utils.collections import find
from polaris.utils.exceptions import ProcessingException

logger = getLogger('polaris.analytics.db.model')
Base = db.polaris_declarative_base(schema='analytics')

# many-many relationship table
organizations_contributors = Table(
    'organizations_contributors', Base.metadata,
    Column('organization_id', ForeignKey('organizations.id'), primary_key=True),
    Column('contributor_id', ForeignKey('contributors.id'), primary_key=True)
)

projects_repositories = Table(
    'projects_repositories', Base.metadata,
    Column('project_id', ForeignKey('projects.id'), primary_key=True),
    Column('repository_id', ForeignKey('repositories.id'), primary_key=True)
)

accounts_organizations = Table(
    'accounts_organizations', Base.metadata,
    Column('account_id', ForeignKey('accounts.id'), primary_key=True, index=True),
    Column('organization_id', ForeignKey('organizations.id'), primary_key=True, index=True)
)

work_items_commits = Table(
    'work_items_commits', Base.metadata,
    Column('work_item_id', ForeignKey('work_items.id'), primary_key=True, index=True),
    Column('commit_id', ForeignKey('commits.id'), primary_key=True, index=True),
    # Note: this is not a primary key since we only want a commit to be mapped
    # to a single delivery cycle for a given work item
    Column('delivery_cycle_id', ForeignKey('work_item_delivery_cycles.delivery_cycle_id', ondelete="SET NULL"),  index=True, nullable=True)
)

work_items_pull_requests = Table(
    'work_items_pull_requests', Base.metadata,
    Column('work_item_id', ForeignKey('work_items.id'), primary_key=True, index=True),
    Column('pull_request_id', ForeignKey('pull_requests.id'), primary_key=True, index=True)
)

repositories_contributor_aliases = Table(
    'repositories_contributor_aliases', Base.metadata,
    Column('repository_id', Integer, ForeignKey('repositories.id', ondelete='CASCADE'), primary_key=True),
    Column('contributor_alias_id', Integer, ForeignKey('contributor_aliases.id', ondelete='CASCADE'), primary_key=True),
    Column('earliest_commit', DateTime, nullable=True),
    Column('latest_commit', DateTime, nullable=True),
    Column('commit_count', Integer, nullable=True),
    # De-normalized columns to generate quicker aggregates of contributors for repositories on up.
    Column('contributor_id', Integer, ForeignKey('contributors.id', ondelete='CASCADE'), nullable=True),
    Column('robot', Boolean, nullable=True),
    Index('ix_repositories_contributor_aliasesrepositoryidcontributorid', 'repository_id', 'contributor_id')
)


class Account(Base):
    __tablename__ = 'accounts'

    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), nullable=False, unique=True)
    name = Column(String, nullable=False)
    profile = Column(JSONB, nullable=False, server_default='{}')
    created = Column(DateTime, nullable=True)
    updated = Column(DateTime, nullable=True)
    # need to update this to non null
    owner_key = Column(UUID(as_uuid=True), nullable=True)

    organizations = relationship("Organization",
                                 secondary=accounts_organizations,
                                 back_populates="accounts")

    members = relationship("AccountMember", back_populates="account")

    @classmethod
    def create(cls, name, key=None, profile=None, owner=None):
        account = Account(
            key=key or uuid.uuid4(),
            name=name,
            profile=profile or {},
            created=datetime.utcnow()
        )
        if owner:
            account.set_owner(owner)

        return account

    @classmethod
    def find_by_account_key(cls, session, account_key):
        return session.query(cls).filter(cls.key == account_key).first()

    @classmethod
    def find_by_name(cls, session, name):
        return session.query(cls).filter(cls.name == name).first()

    def is_member(self, user):
        for member in self.members:
            if member.user_key == user.key:
                return True

    def add_member(self, user, role=AccountRoles.member):
        if not self.is_member(user):
            self.members.append(
                AccountMember(
                    user_key=user.key,
                    role=role.value
                )
            )
            return True

    def create_organization(self, name, key=None, profile=None, owner=None):
        organization = Organization.create(name, key, profile)
        self.organizations.append(organization)
        if owner:
            organization.set_owner(owner)
        return organization

    def set_owner(self, user):
        self.add_member(user, AccountRoles.owner)
        self.owner_key = user.key


accounts = Account.__table__


class AccountMember(Base):
    __tablename__ = 'account_members'

    account_id = Column(Integer, ForeignKey('accounts.id'), primary_key=True)
    user_key = Column(UUID(as_uuid=True), primary_key=True)
    role = Column(String, nullable=False, server_default=AccountRoles.member.value)

    account = relationship('Account', back_populates='members')


account_members = AccountMember.__table__


class Organization(Base):
    __tablename__ = 'organizations'

    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), nullable=False, unique=True)
    name = Column(String(256))
    public = Column(Boolean, default=False, nullable=True)
    profile = Column(JSONB, nullable=True, server_default='{}')

    created = Column(DateTime, nullable=True)
    updated = Column(DateTime, nullable=True)

    accounts = relationship('Account',
                            secondary=accounts_organizations,
                            back_populates="organizations")

    projects = relationship('Project')
    contributors = relationship('Contributor', secondary=organizations_contributors, back_populates='organizations')
    repositories = relationship('Repository')
    work_items_sources = relationship('WorkItemsSource')
    members = relationship("OrganizationMember", back_populates="organization")

    @classmethod
    def create(cls, name, key=None, profile=None):
        organization = Organization(
            key=key or uuid.uuid4(),
            name=name,
            profile=profile or {},
            created=datetime.utcnow()
        )
        return organization

    @classmethod
    def find_all(cls, session):
        return session.query(cls).all()

    @classmethod
    def find_by_name(cls, session, organization_name):
        return session.query(cls).filter(cls.name == organization_name).first()

    @classmethod
    def find_by_organization_key(cls, session, organization_key):
        return session.query(cls).filter(cls.key == organization_key).first()

    @classmethod
    def find_by_organization_keys(cls, session, organization_keys):
        return session.query(cls).filter(cls.key.in_(organization_keys)).all()

    def belongs_to_account(self, account):
        for ac in self.accounts:
            if ac.key == account.key:
                return True

    def is_member(self, user):
        for member in self.members:
            if member.user_key == user.key:
                return True

    def add_member(self, user, role=OrganizationRoles.member):
        if not self.is_member(user):
            self.members.append(
                OrganizationMember(
                    user_key=user.key,
                    role=role.value
                )
            )
            return True

    def set_owner(self, user):
        self.add_member(user, OrganizationRoles.owner)

    def add_or_update_project(self, name, project_key=None, properties=None, repositories=None):
        existing = find(self.projects, lambda project: project.name == name)
        if not existing:
            project = Project.create(
                name,
                project_key=project_key,
                properties=properties
            )
            self.projects.append(project)
            object_session(self).flush()
            if repositories is not None:
                project.update_repositories(repositories)

        else:
            if properties is not None:
                existing.properties = properties
            if repositories is not None:
                existing.update_repositories(repositories)

        return not existing


organizations = Organization.__table__


class OrganizationMember(Base):
    __tablename__ = 'organization_members'

    organization_id = Column(Integer, ForeignKey('organizations.id'), primary_key=True)
    user_key = Column(UUID(as_uuid=True), primary_key=True)
    role = Column(String, nullable=False, server_default=OrganizationRoles.member.value)

    organization = relationship('Organization', back_populates='members')


organization_members = OrganizationMember.__table__


class Project(Base):
    __tablename__ = 'projects'

    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True)
    name = Column(String(256), nullable=False)
    public = Column(Boolean, default=False, nullable=True)
    properties = Column(JSONB, default={})
    archived = Column(Boolean, server_default=text('FALSE'), nullable=False)
    settings = Column(JSONB, server_default=text("'{}'"), nullable=True)

    organization_id = Column(Integer, ForeignKey('organizations.id'))
    organization = relationship('Organization', back_populates='projects')
    repositories = relationship('Repository', secondary=projects_repositories, back_populates='projects')
    work_items_sources = relationship('WorkItemsSource')

    @classmethod
    def find_by_project_key(cls, session, project_key):
        return session.query(cls).filter(cls.key == project_key).first()

    @classmethod
    def create(cls, name, project_key=None, properties=None):
        project = Project(
            name=name,
            project_key=uuid.uuid4() if project_key is None else project_key,
            properties={} if properties is None else properties
        )
        return project

    def update_repositories(self, repo_names):
        repo_instances = Repository.find_repositories_by_name(object_session(self), self.organization.organization_key,
                                                              repo_names)
        if len(repo_instances) < len(repo_names):
            logger.warning("One or more repositories in the list of provided repo_names were not found when adding"
                           " repositories to project {} by name".format(self.name))

        for repo in repo_instances:
            if repo not in self.repositories:
                logger.info("Adding repo {} to project  {}".format(repo.name, self.name))
                self.repositories.append(repo)

    def update_settings(self, update_project_settings_input):
        # we have to make a deep copy here since sqlalchemy does not recognize in place modifications
        # to the dict in the jsonb field
        current = copy.deepcopy(self.settings) if self.settings is not None else dict()
        if update_project_settings_input.flow_metrics_settings:
            flow_metrics_input = update_project_settings_input.flow_metrics_settings

            if 'flow_metrics_settings' not in current:
                current['flow_metrics_settings'] = dict()

            flow_metrics_settings = current['flow_metrics_settings']

            if flow_metrics_input.lead_time_target:
                flow_metrics_settings['lead_time_target'] = flow_metrics_input.lead_time_target

            if flow_metrics_input.cycle_time_target:
                flow_metrics_settings['cycle_time_target'] = flow_metrics_input.cycle_time_target

            if flow_metrics_input.response_time_confidence_target:
                flow_metrics_settings['response_time_confidence_target'] = flow_metrics_input.response_time_confidence_target

            if flow_metrics_input.lead_time_confidence_target:
                flow_metrics_settings['lead_time_confidence_target'] = flow_metrics_input.lead_time_confidence_target

            if flow_metrics_input.cycle_time_confidence_target:
                flow_metrics_settings['cycle_time_confidence_target'] = flow_metrics_input.cycle_time_confidence_target

        self.settings = current


projects = Project.__table__


class Repository(Base):
    __tablename__ = 'repositories'

    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True, nullable=False)
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)

    url = Column(String(256), nullable=True)
    public = Column(Boolean, nullable=True, default=False)
    integration_type = Column(String, nullable=True)

    # id of the repository in the source system that it was imported from.
    source_id = Column(String, nullable=True)

    properties = Column(JSONB, default={}, nullable=True)
    earliest_commit = Column(DateTime, nullable=True)
    latest_commit = Column(DateTime, nullable=True)
    commit_count = Column(BigInteger, nullable=True)

    # parent
    organization_id = Column(Integer, ForeignKey('organizations.id'), index=True, nullable=False)
    organization = relationship('Organization', back_populates='repositories')

    # relationships
    commits = relationship('Commit', back_populates='repository')
    projects = relationship('Project', secondary=projects_repositories, back_populates='repositories')
    source_files = relationship('SourceFile', back_populates='repository')
    source_file_changes = relationship('WorkItemSourceFileChange', cascade='all, delete-orphan')

    @classmethod
    def find_by_repository_key(cls, session, repository_key):
        return session.query(cls).filter(cls.key == repository_key).first()

    @classmethod
    def find_by_source_id(cls, session, source_id):
        return session.query(cls).filter(cls.source_id == source_id).first()

    @classmethod
    def find_repositories_by_name(cls, session, organization_key, repo_names):
        repos = cls.__table__
        return session.query(cls).filter(
            and_(
                repos.c.organization_key == db.uuid_hex(organization_key),
                repos.c.name.in_(repo_names)
            )
        ).all()

    @classmethod
    def find_repository_by_name(cls, session, organization_key, repo_name):
        repos = cls.__table__
        return session.query(cls).filter(
            and_(
                repos.c.organization_key == db.uuid_hex(organization_key),
                repos.c.name == repo_name
            )
        ).first()

    @classmethod
    def create(cls, session, organization_key, repository, url, **kwargs):
        organization = Organization.find_by_organization_key(session, organization_key)
        if organization:
            repository = Repository(
                organization=organization,
                organization_key=db.uuid_hex(organization_key),
                key=db.uuid_hex(uuid.uuid4()),
                name=repository,
                url=url,
                public=kwargs.get('visibility', 'private') == 'public'
            )
            session.add(repository)
            session.flush()
            return repository

    @classmethod
    def update(cls, instance, **kwargs):
        if kwargs.get('repository'):
            instance.name = kwargs['repository']
        if kwargs.get('url'):
            instance.url = kwargs['url']
        if kwargs.get('vendor'):
            instance.vendor = kwargs['vendor']

        if kwargs.get('properties'):
            instance.properties = {*instance.properties, *kwargs['properties']}


repositories = Repository.__table__
UniqueConstraint(repositories.c.organization_id, repositories.c.name)


class Contributor(Base):
    __tablename__ = 'contributors'

    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True, nullable=False)
    name = Column(String, nullable=False)

    aliases = relationship('ContributorAlias', back_populates='contributor')
    organizations = relationship('Organization', secondary=organizations_contributors, back_populates='contributors')

    @classmethod
    def find_by_contributor_key(cls, session, contributor_key):
        return session.query(cls).filter(cls.key == contributor_key).first()


contributors = Contributor.__table__


class ContributorAlias(Base):
    __tablename__ = 'contributor_aliases'
    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True, nullable=False)
    name = Column(String, nullable=False)
    source = Column(String, nullable=False)
    source_alias = Column(String, nullable=False)
    robot = Column(Boolean, nullable=False, default=False, server_default=text('FALSE'))

    contributor_id = Column(Integer, ForeignKey('contributors.id'), index=True, nullable=False)
    contributor = relationship('Contributor', back_populates='aliases')
    delivery_cycles = relationship('WorkItemDeliveryCycleContributor', cascade="all, delete-orphan")

    @classmethod
    def find_by_contributor_alias_key(cls, session, contributor_alias_key):
        return session.query(cls).filter(cls.key == contributor_alias_key).first()


contributor_aliases = ContributorAlias.__table__


class Commit(Base):
    __tablename__ = 'commits'

    id = Column(BigInteger, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True, nullable=False)
    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)

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
    stats = Column(JSONB, nullable=True)

    source_files = Column(JSONB, nullable=True)
    source_file_types_summary = Column(JSONB, nullable=True)
    source_file_actions_summary = Column(JSONB, nullable=True)

    work_items_summaries = Column(JSONB, nullable=True, server_default='[]')

    # relationships
    repository = relationship('Repository', back_populates='commits')
    committer_alias = relationship('ContributorAlias', foreign_keys=[committer_contributor_alias_id])
    author_alias = relationship('ContributorAlias', foreign_keys=[author_contributor_alias_id])

    work_items = relationship('WorkItem',
                              secondary=work_items_commits,
                              back_populates="commits")

    # Work Items Source File Relationship
    source_file_changes = relationship('WorkItemSourceFileChange', cascade='all, delete-orphan')

    @classmethod
    def find_by_commit_key(cls, session, commit_key):
        return session.query(cls).filter(cls.key == commit_key).first()

    def add_work_item_summary(self, work_item_summary):
        if self.work_items_summaries is None:
            self.work_items_summaries = [work_item_summary]
        else:
            if not find(self.work_items_summaries, lambda work_item: work_item['key'] == work_item_summary['key']):
                self.work_items_summaries = [*self.work_items_summaries, work_item_summary]


commits = Commit.__table__

UniqueConstraint(commits.c.repository_id, commits.c.source_commit_id)
Index('ix_analytics_commits_author_committer_contributor_key', commits.c.author_contributor_key,
      commits.c.committer_contributor_key)


class SourceFile(Base):
    __tablename__ = 'source_files'

    id = Column(BigInteger, primary_key=True)
    key = Column(UUID(as_uuid=True), unique=True, nullable=False)
    name = Column(String, nullable=False, default='')
    path = Column(String, nullable=False, default='')
    file_type = Column(String, nullable=False, default='')
    version_count = Column(Integer, nullable=False, default=1)

    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)
    repository = relationship('Repository', back_populates='source_files')

    # Work Items Source File Relationship
    source_file_changes = relationship('WorkItemSourceFileChange', cascade='all, delete-orphan')


source_files = SourceFile.__table__


# -------------------------------------
# Work Tracking
# --------------------------------------


class WorkItemsSource(Base):
    __tablename__ = 'work_items_sources'

    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), nullable=False, unique=True)
    # User facing display name for the instance.
    name = Column(String, nullable=False)
    organization_key = Column(UUID(as_uuid=True), nullable=False)
    # type of integration: github, github_enterprise, jira, pivotal_tracker etc..
    integration_type = Column(String, nullable=False)
    # subtype of the integration type
    work_items_source_type = Column(String, nullable=True)

    description = Column(String, nullable=True)
    # the id of the external source from which this work item source was imported.
    source_id = Column(String, nullable=True)
    # Commit mapping scope specifies the repositories that are mapped to this
    # work item source. The valid values are ('organization', 'project', 'repository')
    # Given the commit mapping scope key, commits originating from all repositories
    # within that specific scope (instance of org, project or repository) will be evaluated to
    # see if they can be mapped to a given work item originating from this work items source.
    commit_mapping_scope = Column(String, nullable=False, default='organization', server_default="'organization'")
    commit_mapping_scope_key = Column(UUID(as_uuid=True), nullable=True)

    organization_id = Column(Integer, ForeignKey('organizations.id'), nullable=False)
    organization = relationship('Organization', back_populates='work_items_sources')

    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)
    project = relationship('Project', back_populates='work_items_sources')

    work_items = relationship('WorkItem')
    delivery_cycles = relationship('WorkItemDeliveryCycle')
    state_maps = relationship('WorkItemsSourceStateMap', cascade="all, delete-orphan")

    @classmethod
    def find_by_organization_key(cls, session, organization_key):
        return session.query(cls).filter(cls.organization_key == organization_key).all()

    @classmethod
    def find_by_work_items_source_key(cls, session, work_items_source_key):
        return session.query(cls).filter(cls.key == work_items_source_key).first()

    @classmethod
    def find_by_commit_mapping_scope(cls, session, organization_key, commit_mapping_scope, commit_mapping_scope_keys):
        return session.query(cls).filter(
            and_(
                cls.organization_key == organization_key,
                cls.commit_mapping_scope == commit_mapping_scope,
                cls.commit_mapping_scope_key.in_(commit_mapping_scope_keys)
            )
        ).all()

    def get_default_state_map(self):
        if self.integration_type == WorkTrackingIntegrationType.github.value:
            return [
                dict(state='created', state_type=WorkItemsStateType.backlog.value),
                dict(state='open', state_type=WorkItemsStateType.backlog.value),
                dict(state='closed', state_type=WorkItemsStateType.closed.value)
            ]
        elif self.integration_type == WorkTrackingIntegrationType.pivotal.value:
            return [
                dict(state='created', state_type=WorkItemsStateType.backlog.value),
                dict(state='unscheduled', state_type=WorkItemsStateType.open.value),
                dict(state='unstarted', state_type=WorkItemsStateType.open.value),
                dict(state='planned', state_type=WorkItemsStateType.open.value),
                dict(state='started', state_type=WorkItemsStateType.wip.value),
                dict(state='finished', state_type=WorkItemsStateType.complete.value),
                dict(state='delivered', state_type=WorkItemsStateType.complete.value),
                dict(state='accepted', state_type=WorkItemsStateType.closed.value)
            ]
        else:
            return []

    def init_state_map(self, state_map_entries=None):
        entries = state_map_entries or self.get_default_state_map()
        if len(entries) >= 0:
            state_maps_keys = [mapping['state'] for mapping in entries]
            if len(set(state_maps_keys)) < len(state_maps_keys):
                raise ProcessingException(f'Invalid state map: duplicate states in the input')

            # We interpolate a created state in case that was not passed in by the caller
            if 'created' not in state_maps_keys:
                entries = [dict(state='created', state_type=WorkItemsStateType.backlog.value), *entries]

        self.state_maps = [
            WorkItemsSourceStateMap(**entry)
            for entry in entries
        ]

    def get_state_type(self, state):
        state_map = find(self.state_maps, lambda sm: sm.state.lower() == state.lower())
        if state_map is not None:
            return state_map.state_type


work_items_sources = WorkItemsSource.__table__
Index('ix_analytics_work_items_sources_commit_mapping_scope',
      work_items_sources.c.organization_key,
      work_items_sources.c.commit_mapping_scope,
      work_items_sources.c.commit_mapping_scope_key
      )


class WorkItem(Base):
    __tablename__ = 'work_items'

    id = Column(BigInteger, primary_key=True)
    key = Column(UUID(as_uuid=True), nullable=False, unique=True)
    name = Column(String(256), nullable=False)
    work_item_type = Column(String, nullable=False)
    display_id = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    is_bug = Column(Boolean, nullable=False, default=False, server_default='FALSE')
    tags = Column(ARRAY(String), nullable=False, default=[], server_default='{}')
    state = Column(String, nullable=True)
    url = Column(String, nullable=True)
    current_delivery_cycle_id = Column(Integer, nullable=True)
    # The id of the entity in a remote system that this is mapped to.
    source_id = Column(String, nullable=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    next_state_seq_no = Column(Integer, nullable=False, server_default='0')

    # Information related to Epic
    is_epic = Column(Boolean, nullable=False, default=False, server_default='FALSE')
    epic_id = Column(Integer, ForeignKey('work_items.id'), nullable=True)
    epic = relationship('WorkItem', remote_side='WorkItem.id')

    # calculated fields

    # state_type based on current state value.
    state_type = Column(String, nullable=True)

    # if state type of current state is 'completed' this is the
    # source time stamp of the update that put it in that state else it is None.
    completed_at = Column(DateTime, nullable=True)

    # commit metrics at the work item level. Since commits can
    # come in "between delivery cycles" we cannot simply aggregate
    # delivery cycle metrics to get commit metrics.
    # We currently have a bit of a gap in how we have implemented this
    # since we are doing everything at the delivery cycle level.
    # We are starting to do maintain this at the work item level
    # starting with the effort metric.

    effort = Column(Float, nullable=True)

    # Work Items Source relationship
    work_items_source_id = Column(Integer, ForeignKey('work_items_sources.id'))
    work_items_source = relationship('WorkItemsSource', back_populates='work_items')
    commits = relationship("Commit",
                           secondary=work_items_commits,
                           back_populates="work_items")

    state_transitions = relationship("WorkItemStateTransition")

    # Work Items Delivery Cycles Relationship
    delivery_cycles = relationship('WorkItemDeliveryCycle', cascade='all, delete-orphan')

    # Work Items Source File Relationship
    source_file_changes = relationship('WorkItemSourceFileChange', cascade='all, delete-orphan')

    @classmethod
    def find_by_work_item_key(cls, session, work_item_key):
        return session.query(cls).filter(cls.key == work_item_key).first()

    @classmethod
    def find_by_work_item_source_key(cls, session, work_items_source_id):
        return session.query(cls).filter(cls.work_items_source_id == work_items_source_id)

    @property
    def current_delivery_cycle(self):
        return object_session(self).query(WorkItemDeliveryCycle).filter(
            WorkItemDeliveryCycle.delivery_cycle_id == self.current_delivery_cycle_id
        ).first()

    def get_summary(self):
        return dict(
            key=self.key.hex,
            name=self.name,
            work_item_type=self.work_item_type,
            display_id=self.display_id,
            url=self.url
        )


work_items = WorkItem.__table__
Index('ix_analytics_work_items_work_items_source_id_display_id',
      work_items.c.work_items_source_id,
      work_items.c.display_id
      )


class WorkItemStateTransition(Base):
    __tablename__ = 'work_item_state_transitions'

    work_item_id = Column(BigInteger, ForeignKey('work_items.id'), primary_key=True)
    seq_no = Column(Integer, primary_key=True, server_default='0')
    created_at = Column(DateTime, nullable=False)
    previous_state = Column(String, nullable=True)
    state = Column(String, nullable=False)

    work_item = relationship("WorkItem", back_populates='state_transitions')


work_item_state_transitions = WorkItemStateTransition.__table__


class WorkItemsSourceStateMap(Base):
    __tablename__ = 'work_items_source_state_map'

    state = Column(String, primary_key=True)
    state_type = Column(String, nullable=False, server_default=WorkItemsStateType.open.value)

    # Work Items Source relationship
    work_items_source_id = Column(Integer, ForeignKey('work_items_sources.id'), primary_key=True)
    work_items_source = relationship('WorkItemsSource', back_populates='state_maps')


work_items_source_state_map = WorkItemsSourceStateMap.__table__


class WorkItemDeliveryCycle(Base):
    __tablename__ = 'work_item_delivery_cycles'

    delivery_cycle_id = Column(Integer, primary_key=True)
    start_seq_no = Column(Integer, nullable=False)
    end_seq_no = Column(Integer, nullable=True)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=True)
    lead_time = Column(Integer, nullable=True)
    cycle_time = Column(Integer, nullable=True)
    earliest_commit = Column(DateTime, nullable=True)
    latest_commit = Column(DateTime, nullable=True)
    repository_count = Column(Integer, nullable=True)
    commit_count = Column(Integer, nullable=True)

    # This the effort associated with commits within the delivery cycle
    # we also have a "global" effort that applies to the work item and
    # includes commits that appear across delivery cycles.
    effort = Column(Float, nullable=True)

    # non-merge commits' code change stats columns
    total_lines_changed_non_merge = Column(Integer, nullable=True)
    total_files_changed_non_merge = Column(Integer, nullable=True)
    total_lines_deleted_non_merge = Column(Integer, nullable=True)
    total_lines_inserted_non_merge = Column(Integer, nullable=True)

    # merge commits' code change stats columns
    total_lines_changed_merge = Column(Integer, nullable=True)
    total_files_changed_merge = Column(Integer, nullable=True)
    average_lines_changed_merge = Column(Integer, nullable=True)

    # Work Items Source relationship
    work_items_source_id = Column(Integer, ForeignKey('work_items_sources.id'), nullable=False, index=True)
    work_items_source = relationship('WorkItemsSource', back_populates='delivery_cycles')

    # Work Items relationship
    work_item_id = Column(Integer, ForeignKey('work_items.id'), nullable=False, index=True)
    work_item = relationship('WorkItem', back_populates='delivery_cycles')

    delivery_cycle_durations = relationship('WorkItemDeliveryCycleDuration', cascade="all, delete-orphan")
    delivery_cycle_contributors = relationship('WorkItemDeliveryCycleContributor', cascade="all, delete-orphan")

    # Work Items Source File Relationship
    source_file_changes = relationship('WorkItemSourceFileChange', cascade='all, delete-orphan')

    commits = relationship("Commit", secondary=work_items_commits)


work_item_delivery_cycles = WorkItemDeliveryCycle.__table__


class WorkItemDeliveryCycleDuration(Base):
    __tablename__ = 'work_item_delivery_cycle_durations'

    state = Column(String, primary_key=True)
    cumulative_time_in_state = Column(Integer, nullable=True)

    # Work Item Delivery Cycles relationship
    delivery_cycle_id = Column(Integer, ForeignKey('work_item_delivery_cycles.delivery_cycle_id'),
                               primary_key=True, index=True, nullable=False)


work_item_delivery_cycle_durations = WorkItemDeliveryCycleDuration.__table__


class WorkItemDeliveryCycleContributor(Base):
    __tablename__ = 'work_item_delivery_cycle_contributors'

    total_lines_as_author = Column(Integer, nullable=True)
    total_lines_as_reviewer = Column(Integer, nullable=True)
    # Work Item Delivery Cycles relationship
    delivery_cycle_id = Column(Integer, ForeignKey('work_item_delivery_cycles.delivery_cycle_id'),
                               primary_key=True, index=True, nullable=False)
    # Contributor Alias relationship
    contributor_alias_id = Column(Integer, ForeignKey('contributor_aliases.id'), primary_key=True, nullable=False)
    contributor_alias = relationship('ContributorAlias', back_populates='delivery_cycles')


work_item_delivery_cycle_contributors = WorkItemDeliveryCycleContributor.__table__


class WorkItemSourceFileChange(Base):
    __tablename__ = 'work_item_source_file_changes'

    id = Column(Integer, primary_key=True)
    source_commit_id = Column(String, nullable=False)
    commit_date = Column(DateTime, nullable=False)
    committer_contributor_alias_id = Column(Integer, nullable=False)
    author_contributor_alias_id = Column(Integer, nullable=False)
    num_parents = Column(Integer, default=1)
    created_on_branch = Column(String, nullable=True)
    file_action = Column(String, nullable=False)
    total_lines_changed = Column(Integer, nullable=True)
    total_lines_deleted = Column(Integer, nullable=True)
    total_lines_added = Column(Integer, nullable=True)

    # relationships
    work_item_id = Column(Integer, ForeignKey('work_items.id'), nullable=False)
    work_item = relationship('WorkItem', back_populates='source_file_changes')
    delivery_cycle_id = Column(Integer, ForeignKey('work_item_delivery_cycles.delivery_cycle_id'), index=True,
                               nullable=True)
    delivery_cycle = relationship('WorkItemDeliveryCycle', back_populates='source_file_changes')
    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)
    repository = relationship('Repository', back_populates='source_file_changes')
    source_file_id = Column(BigInteger, ForeignKey('source_files.id'), nullable=False)
    source_file = relationship('SourceFile', back_populates='source_file_changes')
    commit_id = Column(BigInteger, ForeignKey('commits.id'), nullable=False)
    commit = relationship('Commit', back_populates='source_file_changes')


work_item_source_file_changes = WorkItemSourceFileChange.__table__
UniqueConstraint(work_item_source_file_changes.c.work_item_id, work_item_source_file_changes.c.delivery_cycle_id, \
                 work_item_source_file_changes.c.repository_id, work_item_source_file_changes.c.commit_id, \
                 work_item_source_file_changes.c.source_file_id)


class PullRequest(Base):
    __tablename__ = 'pull_requests'

    id = Column(Integer, primary_key=True)
    key = Column(UUID(as_uuid=True), nullable=False, unique=True)

    # ID of the item in the source system, used to cross ref this instance for updates etc.
    source_id = Column(String, nullable=False)
    display_id = Column(String, nullable=True)
    # PR details
    title = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    # PR web url
    web_url = Column(String, nullable=True)

    # timestamps for synchronization
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=True)
    # Date when system marks PR for deletion
    deleted_at = Column(DateTime, nullable=True)

    # State and status updates from source system
    source_state = Column(String, nullable=False)
    merge_status = Column(String, nullable=True)
    merged_at = Column(DateTime, nullable=True)

    state = Column(String, nullable=True)

    # Source(Branch/Repo from which PR originates) and target branch and repository details
    source_branch = Column(String, nullable=False)
    target_branch = Column(String, nullable=False)

    source_repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=True)

    # Repository Relationship
    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)
    repository = relationship('Repository', foreign_keys=[repository_id])

    source_repository = relationship('Repository', foreign_keys=[source_repository_id])


pull_requests = PullRequest.__table__
UniqueConstraint(pull_requests.c.repository_id, pull_requests.c.source_id)


class FeatureFlag(Base):
    __tablename__ = 'feature_flags'

    id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False, unique=True)
    key = Column(UUID(as_uuid=True), nullable=False, unique=True)
    active = Column(Boolean, nullable=False, default=True, server_default=text('TRUE'))
    created = Column(DateTime, nullable=False)
    updated = Column(DateTime)
    enable_all = Column(Boolean, nullable=False, default=False, server_default=text('FALSE'))
    enable_all_date = Column(DateTime)
    deactivated_date = Column(DateTime)
    enablements = relationship('FeatureFlagEnablement', cascade='all, delete-orphan')

    @classmethod
    def create(cls, name, key=None):
        feature_flag = FeatureFlag(
            key=key or uuid.uuid4(),
            name=name,
            created=datetime.utcnow()
        )
        return feature_flag

    @classmethod
    def find_by_key(cls, session, key):
        return session.query(cls).filter(cls.key == key).first()

    @classmethod
    def find_by_name(cls, session, name):
        return session.query(cls).filter(cls.name == name).first()


feature_flags = FeatureFlag.__table__


class FeatureFlagEnablement(Base):
    __tablename__ = 'feature_flag_enablements'

    scope = Column(String(256), nullable=False)
    scope_key = Column(UUID(as_uuid=True), nullable=False, primary_key=True)
    enabled = Column(Boolean, nullable=False, default=False, server_default='FALSE')

    # Feature flags relationship
    feature_flag_id = Column(Integer, ForeignKey('feature_flags.id'), primary_key=True)
    feature_flags = relationship('FeatureFlag', back_populates='enablements')


feature_flag_enablements = FeatureFlagEnablement.__table__


def recreate_all(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
