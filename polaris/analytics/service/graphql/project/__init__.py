# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from .project_repositories import ProjectRepositories
from .project_commit_summary import ProjectCommitSummary

from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from polaris.analytics.service.graphql.mixins import \
    KeyIdResolverMixin, \
    NamedNodeResolverMixin, \
    CommitSummaryResolverMixin

from polaris.common import db
from polaris.repos.db.model import Project as ProjectModel


class Project(
    KeyIdResolverMixin,
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    repositories = graphene.Field(ProjectRepositories)

    @classmethod
    def resolve_field(cls, parent, info, project_key, **kwargs):
        return Project(key=project_key)

    @staticmethod
    def load_instance(key, info, **kwargs):
        with db.orm_session() as session:
            return ProjectModel.find_by_project_key(session, key)

    def resolve_repositories(self, info, **kwargs):
        return ProjectRepositories.resolve(self, info, **kwargs)

    def resolve_commit_summary(self, info, **kwargs):
            return ProjectCommitSummary.resolve(self.key, info, **kwargs)

