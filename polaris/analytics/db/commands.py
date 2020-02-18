# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from polaris.analytics.db import impl
from sqlalchemy.exc import SQLAlchemyError


from .api import success


def update_commit_work_item_summaries(organization_key, work_item_commits):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_commit_work_item_summaries(
                    session, organization_key, work_item_commits)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update commit work item summaries failed", exc)
    except Exception as e:
        return db.failure_message('Update commit work item summaries failed', e)


def update_project_work_items_source_state_mappings(project_state_mappings, join_this=None):
    with db.orm_session(join_this) as session:
        work_items_source = impl.update_project_work_items_source_state_mappings(session, project_state_mappings)
        # Syncing all related work items' state_type
        impl.sync_work_items_state_mappings(session, work_items_source)
        if work_items_source:
            return True

def infer_projects_repositories_relationships(organization_key, work_items_commits):
    try:
        with db.orm_session() as session:
            return success(
                impl.infer_projects_repositories_relationships(
                    session, organization_key, work_items_commits)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update commit work item summaries failed", exc)
    except Exception as e:
        return db.failure_message('Update commit work item summaries failed', e)


def resolve_work_items_sources_for_repositories(organization_key, repositories):
    try:
        with db.orm_session() as session:
            return success(
                impl.resolve_repository_commit_mapping_scope_from_repositories(
                    session, organization_key, repositories)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Resolve work items sources for repositories failed", exc)
    except Exception as e:
        return db.failure_message('Resolve work items sources for repositories failed', e)


