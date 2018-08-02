# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene

from .account_repositories_resolvers import *
from ..mixins import NamedNodeCountResolverMixin
from ..repository import Repository
from ..utils import resolve_collection


class AccountRepositories(
    NamedNodeCountResolverMixin,
    graphene.ObjectType
):

    def __init__(self, account, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account = account

    count = graphene.Field(graphene.Int)
    nodes = graphene.Field(
        graphene.List(Repository),
        interfaces=graphene.Argument(
            graphene.List(graphene.Enum(
                'AccountRepositoriesInterfaces', [
                    ('CommitSummary', 'CommitSummary'),
                    ('ContributorSummary', 'ContributorSummary')
                ]
            )),
            required=False,
        )
    )

    InterfaceResolvers = {
        'NamedNode': AccountRepositoriesNodes,
        'CommitSummary': AccountRepositoriesCommitSummaries,
        'ContributorSummary': AccountRepositoriesContributorSummaries
    }

    @classmethod
    def Field(cls):
        return graphene.Field(AccountRepositories)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        return AccountRepositories(account=account)

    def get_nodes_query_params(self):
        return dict(account_key=self.account.key)

    def resolve_nodes(self, info, **kwargs):
        return resolve_collection(
            self.InterfaceResolvers,
            resolver_context='account_repositories',
            params=self.get_nodes_query_params(),
            output_type=Repository,
            **kwargs
        )