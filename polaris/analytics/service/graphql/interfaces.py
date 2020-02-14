# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from polaris.graphql.interfaces import NamedNode

from polaris.common.enums import WorkTrackingIntegrationType as _WorkTrackingIntegrationType

WorkTrackingIntegrationType = graphene.Enum.from_enum(_WorkTrackingIntegrationType)


class FileTypesSummary(graphene.ObjectType):
    file_type = graphene.String(required=True)
    count = graphene.Int(required=True)


class CommitWorkItemsSummary(graphene.ObjectType):
    key = graphene.String(required=True)
    name = graphene.String(required=True)
    label = graphene.String(required=True)
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
    num_parents = graphene.Int(required=False)
    branch = graphene.String(required=False)
    stats = graphene.Field(CommitChangeStats, required=False)
    file_types_summary = graphene.Field(graphene.List(FileTypesSummary, required=False))
    work_items_summaries = graphene.Field(graphene.List(CommitWorkItemsSummary, required=False))


class WorkItemCommitInfo(CommitInfo):
    commit_key = graphene.String(required=True)
    work_item_name = graphene.String(required=True)
    work_item_key = graphene.String(required=True)


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


class WorkItemsSourceRef(graphene.Interface):
    work_items_source_name = graphene.String(required=True)
    work_items_source_key = graphene.String(required=True)


class WorkItemInfo(graphene.Interface):
    work_item_type = graphene.String(required=True)
    display_id = graphene.String(required=True)
    url = graphene.String(required=True)
    description = graphene.String(required=False)
    state = graphene.String(required=True)
    tags = graphene.Field(graphene.List(graphene.String, required=True))
    created_at = graphene.DateTime(required=True)
    updated_at = graphene.DateTime(required=True)
    state_type = graphene.String(required=False)


class WorkItemStateTransition(graphene.Interface):
    event_date = graphene.DateTime(required=True)
    seq_no = graphene.Int(required=True)
    previous_state = graphene.String(required=False)
    new_state = graphene.String(required=True)


class WorkItemEventSpan(graphene.Interface):
    earliest_work_item_event = graphene.DateTime(required=False)
    latest_work_item_event = graphene.DateTime(required=False)


class WorkItemStateMapping(graphene.Interface):
    state = graphene.String(required=True)
    state_type = graphene.String(required=False)


class AccountInfo(graphene.Interface):
    created = graphene.DateTime(required=False)
    updated = graphene.DateTime(required=False)


class UserInfo(graphene.Interface):
    name = graphene.String(required=True)
    first_name = graphene.String(required=True)
    last_name = graphene.String(required=True)
    email = graphene.String(required=True)


class OwnerInfo(graphene.Interface):
    owner_key = graphene.String(required=True)


class ScopedRole(graphene.Interface):
    scope_key = graphene.String(required=True)
    role = graphene.String(required=True)


class ArchivedStatus(graphene.Interface):
    """Indicate whether the implementing object is archived or not"""

    archived = graphene.Boolean(required=True)


class Describable(graphene.Interface):
    description = graphene.String(required=False)
