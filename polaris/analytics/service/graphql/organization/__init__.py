# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from .organization_resolvers import OrganizationNode, OrganizationCommitSummary, OrganizationContributorSummary
from ..interfaces import CommitSummary, ContributorSummary, NamedNode
from ..utils import resolve_instance

from .organization_projects import OrganizationProjects
from .organization_repositories import OrganizationRepositories

from ..mixins import \
    NamedNodeResolverMixin, \
    CommitSummaryResolverMixin, \
    ContributorSummaryResolverMixin

from polaris.common import db
from polaris.repos.db.model import Organization as OrganizationModel


class Organization(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    projects = graphene.Field(OrganizationProjects)
    repositories = graphene.Field(OrganizationRepositories)

    @classmethod
    def Field(cls):
        return graphene.Field(
            cls,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(graphene.Enum(
                    'OrganizationInterfaces', [
                        ('CommitSummary', 'CommitSummary'),
                        ('ContributorSummary', 'ContributorSummary')
                    ]
                )),
                required=False,
            )
        )

    InterfaceResolvers = {
        'NamedNode': OrganizationNode,
        'CommitSummary': OrganizationCommitSummary,
        'ContributorSummary': OrganizationContributorSummary
    }

    @classmethod
    def resolve_field(cls, parent, info, organization_key, **kwargs):
        return resolve_instance(
            cls.InterfaceResolvers,
            resolver_context='organization',
            params=dict(organization_key=organization_key),
            output_type=cls,
            **kwargs
        )

    @staticmethod
    def load_instance(key, info, **kwargs):
        with db.orm_session() as session:
            return OrganizationModel.find_by_organization_key(session, key)

    def get_node_query_params(self):
        return dict(organization_key=self.key)

    def resolve_projects(self, info, **kwargs):
        return OrganizationProjects.resolve(self, info, **kwargs)

    def resolve_repositories(self, info, **kwargs):
        return OrganizationRepositories.resolve(self, info, **kwargs)

