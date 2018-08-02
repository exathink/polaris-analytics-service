# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene

from ..mixins import NamedNodeCountResolverMixin
from ..repository import Repository
from ..utils import resolve_collection
from .selectables import OrganizationRepositoriesNodes
from ..repository import RepositoriesCommitSummary, RepositoriesContributorSummary

class OrganizationRepositories(
    NamedNodeCountResolverMixin,
    graphene.ObjectType
):

    def __init__(self, organization, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.organization = organization

    count = graphene.Field(graphene.Int)
    nodes = graphene.Field(
        graphene.List(Repository),
        interfaces=graphene.Argument(
            graphene.List(graphene.Enum(
                'OrganizationRepositoriesInterfaces', [
                    ('CommitSummary', 'CommitSummary'),
                    ('ContributorSummary', 'ContributorSummary')
                ]
            )),
            required=False,
        )
    )

    InterfaceResolvers = {
        'NamedNode': OrganizationRepositoriesNodes,
        'CommitSummary': RepositoriesCommitSummary,
        'ContributorSummary': RepositoriesContributorSummary
    }

    @classmethod
    def Field(cls):
        return graphene.Field(OrganizationRepositories)

    @classmethod
    def resolve(cls, organization, info, **kwargs):
        return OrganizationRepositories(organization=organization)

    def get_nodes_query_params(self):
        return dict(organization_key=self.organization.key)

    def resolve_nodes(self, info, **kwargs):
        return resolve_collection(
            self.InterfaceResolvers,
            resolver_context='organization_repositories',
            params=self.get_nodes_query_params(),
            output_type=Repository,
            **kwargs
        )


