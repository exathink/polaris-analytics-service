# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..project import Project
from ..mixins import NamedNodeCountResolverMixin, JoinResolverMixin
from ..utils import resolve_join
from .account_projects_resolvers import *

class AccountProjects(
    NamedNodeCountResolverMixin,
    JoinResolverMixin,
    graphene.ObjectType
):



    def __init__(self, account, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account = account

    count = graphene.Field(graphene.Int)
    nodes = graphene.Field(
        graphene.List(Project),
        interfaces=graphene.Argument(
            graphene.List(graphene.Enum(
                'AccountProjectsInterfaces', [
                    ('CommitSummary', 'CommitSummary'),
                    ('ContributorSummary', 'ContributorSummary')
                ]
            )),
            required=False,
        )
    )

    InterfaceResolvers = {
        'NamedNode': AccountProjectsNodes,
        'CommitSummary': AccountProjectsCommitSummaries,
        'ContributorSummary': AccountProjectsContributorSummaries
    }

    @classmethod
    def Field(cls):
        return graphene.Field(AccountProjects)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        return AccountProjects(account=account)

    def get_nodes_query_params(self):
        return dict(account_key=self.account.key)

    def resolve_nodes(self, info, **kwargs):
        resolvers = self.collect_join_resolvers(info, **kwargs)
        return resolve_join(resolvers, resolver_context='account_projects', output_type=Project,
                            params=self.get_nodes_query_params(), **kwargs)


