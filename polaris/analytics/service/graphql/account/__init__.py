# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from flask_security import current_user

from .account_organizations import AccountOrganizations
from .account_projects import AccountProjects
from .account_repositories import AccountRepositories
from .selectables import AccountNode, AccountCommitSummary, AccountContributorSummary
from ..interfaces import CommitSummary, NamedNode, ContributorSummary
from ..mixins import \
    CommitSummaryResolverMixin, \
    ContributorSummaryResolverMixin
from ..utils import AccessDeniedException
from ..utils import resolve_instance


class Account(
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    organizations = AccountOrganizations.Field()
    projects = graphene.Field(AccountProjects)
    repositories = graphene.Field(AccountRepositories)

    @classmethod
    def Field(cls):
        return graphene.Field(
            cls,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(graphene.Enum(
                    'AccountInterfaces', [
                        ('CommitSummary', 'CommitSummary'),
                        ('ContributorSummary', 'ContributorSummary')
                    ]
                )),
                required=False,
            )
        )

    InterfaceResolvers = {
        'NamedNode': AccountNode,
        'CommitSummary': AccountCommitSummary,
        'ContributorSummary': AccountContributorSummary
    }

    @classmethod
    def resolve_field(cls, info, key, **kwargs):
        if key == str(current_user.account_key):
            return resolve_instance(
                cls.InterfaceResolvers,
                resolver_context='account',
                params=dict(account_key=key),
                output_type=Account,
                **kwargs
            )

        else:
            raise AccessDeniedException('Access denied for specified account')



    def get_node_query_params(self):
        return dict(account_key=self.key)

    def resolve_organizations(self, info, **kwargs):
        return AccountOrganizations.resolve(self, info, **kwargs)

    def resolve_projects(self, info, **kwargs):
        return AccountProjects.resolve(self, info, **kwargs)

    def resolve_repositories(self, info, **kwargs):
        return AccountRepositories.resolve(self, info, **kwargs)
