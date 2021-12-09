# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from flask import abort
from flask_login import current_user
import graphene
from polaris.common import db

from polaris.analytics.db.model import WorkItem
from polaris.analytics import publish

logger = logging.getLogger('polaris.analytics.graphql')


class ResolveCommitsForWorkItemsInput(graphene.InputObjectType):
    organization_key = graphene.String(required=True)
    work_items_source_key = graphene.String(required=True)
    work_item_keys = graphene.List(graphene.String, required=True)


class ResolveCommitsForWorkItems(graphene.Mutation):
    class Arguments:
        resolve_commits_for_work_items_input = ResolveCommitsForWorkItemsInput(required=True)

    success = graphene.Boolean()

    def mutate(self, info, resolve_commits_for_work_items_input):
        # This is really an 'or' clause. Since it is hard to test graphql mutations
        # without flask present, we are simply relying on the fact that current_user is
        # null when we run in the test environment. Theoretically unsafe, but should be ok in
        # any reasonable production scenario. 
        if not current_user or 'admin' in current_user.roles_names:
            with db.orm_session() as session:
                work_item_summaries = []
                for work_item_key in resolve_commits_for_work_items_input.work_item_keys:
                    work_item = WorkItem.find_by_work_item_key(session, work_item_key)
                    work_item_summaries.append(
                        dict(
                            key=work_item.key.hex,
                            name=work_item.name,
                            work_item_type=work_item.work_item_type,
                            display_id=work_item.display_id,
                            url=work_item.url,
                            is_bug=work_item.is_bug,
                            tags=work_item.tags,
                            state=work_item.state,
                            created_at=work_item.created_at,
                            updated_at=work_item.updated_at,
                            deleted_at=work_item.deleted_at,
                            description=work_item.description,
                            source_id=work_item.source_id,
                            is_epic=work_item.is_epic,
                            parent_key=work_item.parent.key if work_item.parent else None,
                            parent_source_display_id=work_item.parent.display_id if work_item.parent else None,
                            commit_identifiers=work_item.commit_identifiers,
                            is_moved_from_current_source=work_item.is_moved_from_current_source
                        )
                    )

                publish.resolve_commits_for_work_items(
                    str(resolve_commits_for_work_items_input.organization_key),
                    str(resolve_commits_for_work_items_input.work_items_source_key),
                    work_item_summaries
                )
                return ResolveCommitsForWorkItems(
                    success=True
                )


class WorkItemsMutationsMixin:
    resolve_commits_for_work_items = ResolveCommitsForWorkItems.Field()
