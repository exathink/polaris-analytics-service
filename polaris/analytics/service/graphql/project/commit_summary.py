# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene



from .commit_summary_for_project import CommitSummaryForProject
from .commit_summary_by_repository import ProjectCommitSummaryByRepository
from .enums import ProjectPartitions

class ProjectCommitSummary(graphene.Union):

    class Meta:
        types = (
            CommitSummaryForProject,
            ProjectCommitSummaryByRepository
        )
    @classmethod
    def Field(cls):
        return graphene.Field(
        graphene.List(ProjectCommitSummary),
        group_by=graphene.Argument(type=ProjectPartitions, required=False, default_value=ProjectPartitions.project.value)
    )

    @classmethod
    def resolve_type(cls, instance, info):
        if instance.type == ProjectPartitions.project.value:
            return CommitSummaryForProject
        elif instance.type == ProjectPartitions.repository.value:
            return ProjectCommitSummaryByRepository


    @classmethod
    def resolve(cls, project, info, **kwargs):
        if kwargs.get('group_by') == ProjectPartitions.project:
            return CommitSummaryForProject.resolve(project, info, **kwargs)
        elif kwargs.get('group_by') == ProjectPartitions.repository:
            return ProjectCommitSummaryByRepository.resolve(project, info, **kwargs)










