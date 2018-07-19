# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene



from .commit_summary_for_project import CommitSummaryForProject
from .commit_summary_by_repository import ProjectCommitSummaryByRepository


class ProjectCommitSummary(graphene.ObjectType):

    project_key = graphene.Field(graphene.UUID)
    for_project = graphene.Field(CommitSummaryForProject)
    by_repository = graphene.Field(graphene.List(ProjectCommitSummaryByRepository))


    @classmethod
    def Field(cls):
        return graphene.Field(ProjectCommitSummary)

    @classmethod
    def resolve(cls, project, info, **kwargs):
        return ProjectCommitSummary(project_key=project.id)


    def resolve_for_project(self, info, **kwargs):
        return CommitSummaryForProject.resolve(self.project_key, info, **kwargs)

    def resolve_by_repository(self, info, **kwargs):
        return ProjectCommitSummaryByRepository.resolve(self.project_key, info, **kwargs)








