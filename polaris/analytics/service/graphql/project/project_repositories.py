# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..repository import Repository
from .project_repositories_commit_summaries import ProjectRepositoriesCommitSummaries
from ..mixins import KeyIdResolverMixin
from .project_repositories_count import ProjectRepositoriesCount

class ProjectRepositories(
    KeyIdResolverMixin,
    graphene.ObjectType
):

    class Meta:
        interfaces = (graphene.relay.Node, )

    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    count = graphene.Field(graphene.Int)
    commit_summaries = graphene.Field(graphene.List(Repository))



    @classmethod
    def Field(cls):
        return graphene.Field(ProjectRepositories)

    @classmethod
    def resolve(cls, project, info, **kwargs):
        return ProjectRepositories(key=project.key)

    def resolve_count(self, info, **kwargs):
        return ProjectRepositoriesCount.resolve(self.key, info, **kwargs)

    def resolve_commit_summaries(self, info, **kwargs):
        return ProjectRepositoriesCommitSummaries.resolve(self.key, info, **kwargs)
