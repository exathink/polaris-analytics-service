# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable

from ..interfaces import CommitSummary, ContributorSummary
from ..mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorSummaryResolverMixin

from ..repository import Repository
from .selectables import ProjectNode, ProjectsContributorSummary, ProjectsCommitSummary, \
    ProjectRepositoriesNodes

from polaris.graphql.connection_utils import CountableConnection, QueryConnectionField


class Project(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)
        named_node_resolver = ProjectNode
        interface_resolvers = {
            'CommitSummary': ProjectsCommitSummary,
            'ContributorSummary': ProjectsContributorSummary
        }
        connection_class = lambda: Projects


    # Child Fields
    repositories = Repository.ConnectionField()


    @classmethod
    def resolve_field(cls, parent, info, project_key, **kwargs):
        return cls.resolve_instance(key=project_key, **kwargs)

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            'project_repositories',
            ProjectRepositoriesNodes,
            self.get_instance_query_params(),
            **kwargs
        )


class Projects(CountableConnection):
    class Meta:
        node = Project
