# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from ..mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorSummaryResolverMixin

from ..project import Project
from ..repository import Repository

from .selectables import OrganizationNode, OrganizationsCommitSummary, \
    OrganizationsContributorSummary, OrganizationProjectsNodes, OrganizationRepositoriesNodes

from polaris.graphql.connection_utils import CountableConnection, QueryConnectionField


class Organization(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    NamedNodeResolver = OrganizationNode
    InterfaceResolvers = {
        'CommitSummary': OrganizationsCommitSummary,
        'ContributorSummary': OrganizationsContributorSummary
    }
    InterfaceEnum = graphene.Enum(
        'OrganizationInterfaces', [
            ('CommitSummary', 'CommitSummary'),
            ('ContributorSummary', 'ContributorSummary')
        ]
    )

    # Child Fields
    projects = Project.ConnectionField()
    repositories = Repository.ConnectionField()

    @classmethod
    def Field(cls):
        return graphene.Field(
            cls,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(cls.InterfaceEnum),
                required=False,
            )
        )

    @classmethod
    def ConnectionField(cls, **kwargs):
        return QueryConnectionField(
            Organizations,
            interfaces=graphene.Argument(
                graphene.List(cls.InterfaceEnum),
                required=False,
            ),
            **kwargs
        )

    @classmethod
    def resolve_field(cls, parent, info, organization_key, **kwargs):
        return cls.resolve_instance(params=dict(organization_key=organization_key), **kwargs)

    def get_node_query_params(self):
        return dict(organization_key=self.key)

    def resolve_projects(self, info, **kwargs):
        return Project.resolve_connection(
            'organization_projects',
            OrganizationProjectsNodes,
            self.get_node_query_params(),
            **kwargs
        )

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            'organization_repositories',
            OrganizationRepositoriesNodes,
            self.get_node_query_params(),
            **kwargs
        )


class Organizations(CountableConnection):
    class Meta:
        node = Organization





