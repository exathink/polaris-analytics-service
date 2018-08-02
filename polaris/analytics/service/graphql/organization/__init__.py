# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from .organization_projects import OrganizationProjects
from .organization_repositories import OrganizationRepositories
from .selectables import OrganizationNode, OrganizationsCommitSummary, OrganizationsContributorSummary
from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from ..mixins import \
    CommitSummaryResolverMixin, \
    ContributorSummaryResolverMixin
from ..utils import resolve_instance


class Organization(
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
        'CommitSummary': OrganizationsCommitSummary,
        'ContributorSummary': OrganizationsContributorSummary
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

    def get_node_query_params(self):
        return dict(organization_key=self.key)

    def resolve_projects(self, info, **kwargs):
        return OrganizationProjects.resolve(self, info, **kwargs)

    def resolve_repositories(self, info, **kwargs):
        return OrganizationRepositories.resolve(self, info, **kwargs)

