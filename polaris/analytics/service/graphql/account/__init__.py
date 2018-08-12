# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from flask_security import current_user

from polaris.graphql.selectable import Selectable

from polaris.graphql.interfaces import NamedNode
from ..interfaces import CommitSummary, ContributorSummary
from ..mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorSummaryResolverMixin

from ..organization import Organization
from ..project import Project
from ..repository import Repository
from ..contributor import Contributor

from .selectables import AccountNode, AccountCommitSummary, AccountContributorSummary, AccountOrganizationsNodes, \
    AccountProjectsNodes, AccountRepositoriesNodes, AccountContributorNodes

from polaris.graphql.exceptions import AccessDeniedException


class Account(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)
        named_node_resolver = AccountNode
        interface_resolvers = {
            'CommitSummary': AccountCommitSummary,
            'ContributorSummary': AccountContributorSummary
        }



    # Child fields
    organizations = Organization.ConnectionField()
    projects = Project.ConnectionField()
    repositories = Repository.ConnectionField()
    contributors = Contributor.ConnectionField()

    @classmethod
    def resolve_field(cls, info, key, **kwargs):
        if key == str(current_user.account_key):
            return cls.resolve_instance(key, **kwargs)
        else:
            raise AccessDeniedException('Access denied for specified account')


    def resolve_organizations(self, info, **kwargs):
        return Organization.resolve_connection(
            'account_organizations',
            AccountOrganizationsNodes,
            self.get_instance_query_params(),
            **kwargs
        )

    def resolve_projects(self, info, **kwargs):
        return Project.resolve_connection(
            'account_projects',
            AccountProjectsNodes,
            self.get_instance_query_params(),
            **kwargs
        )

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            'account_repositories',
            AccountRepositoriesNodes,
            self.get_instance_query_params(),
            **kwargs
        )

    def resolve_contributors(self, info, **kwargs):
        return Contributor.resolve_connection(
            'account_contributors',
            AccountContributorNodes,
            self.get_instance_query_params(),
            **kwargs
        )