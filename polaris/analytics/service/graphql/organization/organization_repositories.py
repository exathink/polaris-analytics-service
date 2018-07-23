# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..repository import Repository
from .organization_repositories_commit_summaries import OrganizationRepositoriesCommitSummaries
from ..mixins import KeyIdResolverMixin

class OrganizationRepositories(
    KeyIdResolverMixin,
    graphene.ObjectType
):

    class Meta:
        interfaces = (graphene.relay.Node, )

    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    commit_summaries = graphene.Field(graphene.List(Repository))



    @classmethod
    def Field(cls):
        return graphene.Field(OrganizationRepositories)

    @classmethod
    def resolve(cls, organization, info, **kwargs):
        return OrganizationRepositories(key=organization.key)

    def resolve_commit_summaries(self, info, **kwargs):
        return OrganizationRepositoriesCommitSummaries.resolve(self.key, info, **kwargs)
