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
from .selectables import ProjectRepositoriesNodes
from ..repository import RepositoriesCommitSummary, RepositoriesContributorSummary

class ProjectRepositories(
    NamedNodeCountResolverMixin,
    graphene.ObjectType
):

    def __init__(self, project, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project = project

    count = graphene.Field(graphene.Int)
    nodes = graphene.Field(
        graphene.List(Repository),
        interfaces=graphene.Argument(
            graphene.List(graphene.Enum(
                'ProjectRepositoriesInterfaces', [
                    ('CommitSummary', 'CommitSummary'),
                    ('ContributorSummary', 'ContributorSummary')
                ]
            )),
            required=False,
        )
    )

    InterfaceResolvers = {
        'NamedNode': ProjectRepositoriesNodes,
        'CommitSummary': RepositoriesCommitSummary,
        'ContributorSummary': RepositoriesContributorSummary
    }

    @classmethod
    def Field(cls):
        return graphene.Field(ProjectRepositories)

    @classmethod
    def resolve(cls, project, info, **kwargs):
        return ProjectRepositories(project=project)

    def get_nodes_query_params(self):
        return dict(project_key=self.project.key)

    def resolve_nodes(self, info, **kwargs):
        return resolve_collection(
            self.InterfaceResolvers,
            resolver_context='project_repositories',
            params=self.get_nodes_query_params(),
            output_type=Repository,
            **kwargs
        )


