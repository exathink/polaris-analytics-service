# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..project import Project
from .account_projects_commit_summaries import AccountProjectsCommitSummaries
from ..mixins import KeyIdResolverMixin

class AccountProjects(
    KeyIdResolverMixin,
    graphene.ObjectType
):

    class Meta:
        interfaces = (graphene.relay.Node, )

    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    commit_summaries = graphene.Field(graphene.List(Project))



    @classmethod
    def Field(cls):
        return graphene.Field(AccountProjects)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        return AccountProjects(key=account.key)

    def resolve_commit_summaries(self, info, **kwargs):
        return AccountProjectsCommitSummaries.resolve(self.key, info, **kwargs)
