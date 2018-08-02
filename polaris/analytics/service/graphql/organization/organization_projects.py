# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..project import Project
from ..mixins import NamedNodeCountResolverMixin
from ..utils import resolve_collection
from .organization_projects_resolvers import *


class OrganizationProjects(
    NamedNodeCountResolverMixin,
    graphene.ObjectType
):

    def __init__(self, organization, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.organization = organization

    count = graphene.Field(graphene.Int)
    nodes = graphene.Field(
        graphene.List(Project),
        interfaces=graphene.Argument(
            graphene.List(graphene.Enum(
                'OrganizationProjectsInterfaces', [
                    ('CommitSummary', 'CommitSummary'),
                    ('ContributorSummary', 'ContributorSummary')
                ]
            )),
            required=False,
        )
    )

    InterfaceResolvers = {
        'NamedNode': OrganizationProjectsNodes,
        'CommitSummary': OrganizationProjectsCommitSummaries,
        'ContributorSummary': OrganizationProjectsContributorSummaries
    }

    @classmethod
    def Field(cls):
        return graphene.Field(OrganizationProjects)

    @classmethod
    def resolve(cls, organization, info, **kwargs):
        return OrganizationProjects(organization=organization)

    def get_nodes_query_params(self):
        return dict(organization_key=self.organization.key)

    def resolve_nodes(self, info, **kwargs):
        return resolve_collection(
            self.InterfaceResolvers,
            resolver_context='organizations_projects',
            params=self.get_nodes_query_params(),
            output_type=Project,
            **kwargs
        )
