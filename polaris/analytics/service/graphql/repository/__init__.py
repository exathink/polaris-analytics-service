# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from .repository_commit_summary import RepositoryCommitSummary

from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from polaris.analytics.service.graphql.mixins import \
    KeyIdResolverMixin, \
    NamedNodeResolverMixin, \
    CommitSummaryResolverMixin

from polaris.common import db
from polaris.repos.db.model import Repository as RepositoryModel


class Repository(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary )



    @classmethod
    def resolve_field(cls, parent, info, repository_key, **kwargs):
        return Repository(key=repository_key)

    @staticmethod
    def load_instance(key, info, **kwargs):
        with db.orm_session() as session:
            return RepositoryModel.find_by_repository_key(session, key)


    def resolve_commit_summary(self, info, **kwargs):
            return RepositoryCommitSummary.resolve(self.key, info, **kwargs)

