# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from .organization_commit_summary import OrganizationCommitSummary
from ..interfaces import CommitSummary, ContributorSummary, NamedNode
from .organization_projects import OrganizationProjects
from .organization_repositories import OrganizationRepositories

from polaris.analytics.service.graphql.mixins import \
    KeyIdResolverMixin, \
    NamedNodeResolverMixin, \
    CommitSummaryResolverMixin, \
    ContributorSummaryResolverMixin

from polaris.common import db
from polaris.repos.db.model import Organization as OrganizationModel

class Organization(
    KeyIdResolverMixin,
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    projects = graphene.Field(OrganizationProjects)
    repositories = graphene.Field(OrganizationRepositories)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def resolve_field(cls, parent, info, organization_key, **kwargs):
        return Organization(key=organization_key)

    @staticmethod
    def load_instance(key, info, **kwargs):
        with db.orm_session() as session:
            return OrganizationModel.find_by_organization_key(session, key)

    def resolve_projects(self, info, **kwargs):
        return OrganizationProjects.resolve(self, info, **kwargs)

    def resolve_repositories(self, info, **kwargs):
        return OrganizationRepositories.resolve(self, info, **kwargs)

    def resolve_commit_summary(self, info, **kwargs):
            return OrganizationCommitSummary.resolve(self.key, info, **kwargs)

