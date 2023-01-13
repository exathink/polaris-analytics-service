# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from polaris.analytics.db import impl
from sqlalchemy.exc import SQLAlchemyError
from polaris.analytics.db.model import Commit


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


def update_work_items_commits_stats(organization_key, work_items_commits):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_work_items_commits_stats(
                    session, organization_key, work_items_commits)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update work items commits span failed", exc)
    except Exception as e:
        return db.failure_message('Update work items commits span failed', e)


def compute_implementation_complexity_metrics_for_work_items(organization_key, work_items_commits):
    try:
        with db.orm_session() as session:
            return success(
                impl.compute_implementation_complexity_metrics_for_work_items(
                    session, organization_key, work_items_commits)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Compute implementation complexity metrics for work items failed", exc)
    except Exception as e:
        return db.failure_message('Compute implementation complexity metrics for work items failed', e)


def compute_implementation_complexity_metrics_for_commits(organization_key, commit_details):
    try:
        with db.orm_session() as session:
            return success(
                impl.compute_implementation_complexity_metrics_for_commits(
                    session, organization_key, commit_details)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Compute implementation complexity metrics for commits failed", exc)
    except Exception as e:
        return db.failure_message('Compute implementation complexity metrics for commits failed', e)


def compute_contributor_metrics_for_work_items(organization_key, work_items_commits):
    try:
        with db.orm_session() as session:
            return success(
                impl.compute_contributor_metrics_for_work_items(
                    session, organization_key, work_items_commits)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Compute contributor metrics for work items failed", exc)
    except Exception as e:
        return db.failure_message('Compute contributor metrics for work items failed', e)


def compute_contributor_metrics_for_commits(organization_key, commit_details):
    try:
        with db.orm_session() as session:
            return success(
                impl.compute_contributor_metrics_for_commits(
                    session, organization_key, commit_details)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Compute contributor metrics for commits failed", exc)
    except Exception as e:
        return db.failure_message('Compute contributor metrics for commits failed', e)


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


def populate_work_items_source_file_changes_for_commits(organization_key, commit_details):
    try:
        with db.orm_session() as session:
            return success(
                impl.populate_work_item_source_file_changes_for_commits(
                    session, commit_details)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Populate work items source file changes for commits failed", exc)
    except Exception as e:
        return db.failure_message('Populate work items source file changes for commits failed', e)


def populate_work_items_source_file_changes_for_work_items(organization_key, work_items_commits):
    try:
        with db.orm_session() as session:
            return success(
                impl.populate_work_item_source_file_changes_for_work_items(
                    session, work_items_commits)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Populate work items source file changes for work items failed", exc)
    except Exception as e:
        return db.failure_message('Populate work items source file changes for work items failed', e)


def assign_contributor_commits_to_teams(organization_key, contributor_team_assignments):
    try:
        with db.orm_session() as session:
            return success(
                impl.assign_contributor_commits_to_teams(
                    session,
                    organization_key,
                    contributor_team_assignments
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Assign contributor commits to teams failed", exc)
    except Exception as e:
        return db.failure_message('Assign contributor commits to teams failed', e)


def resolve_teams_for_work_items(organization_key, work_items_commits):
    try:
        with db.orm_session() as session:
            return success(
                impl.resolve_teams_for_work_items(
                    session,
                    organization_key,
                    work_items_commits
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Resolve teams  for work items failed", exc)
    except Exception as e:
        return db.failure_message('Resolve teams for work items failed', e)


def recalculate_cycle_times_for_work_items_source(work_item_source_key, rebuild_delivery_cycles=True, join_this=None):
    try:
        with db.orm_session(join_this) as session:
            return success(
                impl.recalculate_cycle_times_for_work_items_source(
                    session,
                    work_item_source_key,
                    rebuild_delivery_cycles=rebuild_delivery_cycles
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Recalculate cycle times for work items source failed", exc)
    except Exception as e:
        return db.failure_message('Recalculate cycle times for work items source failed', e)