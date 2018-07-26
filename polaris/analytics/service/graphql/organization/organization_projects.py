# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..project import Project
from .organization_projects_commit_summaries import OrganizationProjectsCommitSummaries
from ..mixins import KeyIdResolverMixin
from .organization_projects_count import OrganizationProjectsCount


class OrganizationProjects(
    KeyIdResolverMixin,
    graphene.ObjectType
):

    class Meta:
        interfaces = (graphene.relay.Node, )

    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    count = graphene.Field(graphene.Int)
    commit_summaries = graphene.Field(graphene.List(Project))



    @classmethod
    def Field(cls):
        return graphene.Field(OrganizationProjects)

    @classmethod
    def resolve(cls, organization, info, **kwargs):
        return OrganizationProjects(key=organization.key)

    def resolve_count(self, info, **kwargs):
        return OrganizationProjectsCount.resolve(self.key, info, **kwargs)

    def resolve_commit_summaries(self, info, **kwargs):
        return OrganizationProjectsCommitSummaries.resolve(self.key, info, **kwargs)
