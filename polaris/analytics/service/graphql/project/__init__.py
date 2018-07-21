# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from .commit_summary_for_project import CommitSummaryForProject

from ..interfaces import CommitSummary, NamedNode
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
        interfaces = (NamedNode, CommitSummary )



    @classmethod
    def resolve_field(cls, parent, info, project_key, **kwargs):
        return Project(key=project_key)

    @staticmethod
    def load_instance(key, info, **kwargs):
        with db.orm_session() as session:
            return ProjectModel.find_by_project_key(session, key)


    def resolve_commit_summary(self, info, **kwargs):
            return CommitSummaryForProject.resolve(self.key, info, **kwargs)

