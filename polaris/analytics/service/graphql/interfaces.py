# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from polaris.graphql.interfaces import NamedNode


class FileTypesSummary(graphene.ObjectType):
    file_type = graphene.String(required=True)
    count = graphene.Int(required=True)


class WorkItemsSummary(graphene.ObjectType):
    label = graphene.String(required=True)
    key = graphene.String(required=True)
    name = graphene.String(required=True)
    work_item_type = graphene.String(required=True)
    display_id = graphene.String(required=True)
    url = graphene.String(required=True)


class CommitChangeStats(graphene.ObjectType):
    lines = graphene.Int(required=True)
    insertions = graphene.Int(required=True)
    deletions = graphene.Int(required=True)
    files = graphene.Int(required=True)


class CommitInfo(NamedNode):
    commit_hash = graphene.String(required=True)
    repository = graphene.String(required=True)
    repository_key = graphene.String(required=True)
    repository_url = graphene.String(required=True)
    commit_date = graphene.DateTime(required=True)
    committer = graphene.String(required=True)
    committer_key = graphene.String(required=True)
    author_date = graphene.DateTime(required=True)
    author = graphene.String(required=True)
    author_key = graphene.String(required=True)
    commit_message = graphene.String(required=True)
    num_parents = graphene.Int(required=True)
    branch = graphene.String(required=False)
    stats = graphene.Field(CommitChangeStats, required=False)
    file_types_summary = graphene.Field(graphene.List(FileTypesSummary, required=False))
    work_items_summaries = graphene.Field(graphene.List(WorkItemsSummary, required=False))


class CumulativeCommitCount(graphene.Interface):
    year = graphene.Int(required=True)
    week = graphene.Int(required=True)
    cumulative_commit_count = graphene.Int(required=True)


class WeeklyContributorCount(graphene.Interface):
    year = graphene.Int(required=True)
    week = graphene.Int(required=True)
    contributor_count = graphene.Int(required=True)


class CommitCount(NamedNode):
    commit_count = graphene.Int()


class CommitSummary(graphene.Interface):
    earliest_commit = graphene.DateTime(required=False)
    latest_commit = graphene.DateTime(required=False)
    commit_count = graphene.Int(required=False, default_value=0)


class ContributorCount(graphene.Interface):
    contributor_count = graphene.Int(required=False, default_value=0)


class ProjectCount(graphene.Interface):
    project_count = graphene.Int(required=False, default_value=0)


class RepositoryCount(graphene.Interface):
    repository_count = graphene.Int(required=False, default_value=0)


class WorkItemsSourceCount(graphene.Interface):
    work_items_source_count = graphene.Int(required=False, default_value=0)


class OrganizationRef(graphene.Interface):
    organization_name = graphene.String(required=True)
    organization_key = graphene.String(required=True)



