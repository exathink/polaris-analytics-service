# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

from polaris.common import db
from polaris.analytics.db import impl
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from .model import WorkItemsSourceStateMap
import uuid
from polaris.utils.collections import dict_select

logger = logging.getLogger('polaris.analytics.db.impl')


def success(result):
    return dict(success=True, **result)


def import_new_commits(organization_key, repository_key, new_commits, new_contributors):
    try:
        with db.create_session() as session:
            return success(
                impl.import_new_commits(session, organization_key, repository_key, new_commits, new_contributors)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Import new_commits failed", exc)
    except Exception as e:
        return db.failure_message('Import new_commits failed', e)


def import_commit_details(organization_key, repository_key, commit_details):
    try:
        with db.orm_session() as session:
            return success(
                impl.import_commit_details(session, repository_key, commit_details)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Import commit details failed", exc)
    except Exception as e:
        return db.failure_message('Import commit details failed', e)


def register_source_file_versions(organization_key, repository_key, commit_details):
    try:
        with db.orm_session() as session:
            return success(
                impl.register_source_file_versions(session, repository_key, commit_details)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Register source file versions failed", exc)
    except Exception as e:
        return db.failure_message('Register source file versions failed', e)


def register_work_items_source(organization_key, work_items_source_summary):
    try:
        with db.orm_session() as session:
            return success(
                impl.register_work_items_source(session, organization_key, work_items_source_summary)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Register work_items_source failed", exc)
    except Exception as e:
        return db.failure_message('Register work_items_source failed', e)


def import_new_work_items(organization_key, work_item_source_key, work_item_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.import_new_work_items(session, work_item_source_key, work_item_summaries)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Import new_work_items failed", exc)
    except Exception as e:
        return db.failure_message('Import new_work_items failed', e)


def resolve_commits_for_new_work_items(organization_key, work_item_source_key, work_item_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.resolve_commits_for_work_items(session, organization_key, work_item_source_key,
                                                    work_item_summaries)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Resolve commits for new work_items", exc)
    except Exception as e:
        return db.failure_message('Resolve commits for new work_items', e)


def resolve_work_items_for_commits(organization_key, repository_key, commit_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.resolve_work_items_for_commits(session, organization_key, repository_key, commit_summaries)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Resolve work items for commits", exc)
    except Exception as e:
        return db.failure_message('Resolve work items for commits', e)


def update_work_items(organization_key, work_item_source_key, work_item_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_work_items(session, work_item_source_key, work_item_summaries)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update new_work_items failed", exc)
    except Exception as e:
        return db.failure_message('Update new_work_items failed', e)


def update_contributor_for_contributor_aliases(organization_key, contributor_key, contributor_alias_keys):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_contributor_for_contributor_aliases(
                    session,
                    organization_key,
                    contributor_key,
                    contributor_alias_keys
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update contributor for contributor aliases", exc)
    except Exception as e:
        return db.failure_message('Update contributor for contributor aliases', e)


def import_project(organization_key, project_summary):
    try:
        with db.orm_session() as session:
            return success(
                impl.import_project(
                    session,
                    organization_key,
                    project_summary
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Import Project", exc)
    except Exception as e:
        return db.failure_message('Import Project', e)


def update_project_work_items_source_state_mappings(project_state_mappings):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_project_work_items_source_state_mappings(
                    session,
                    project_state_mappings
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update project work items source state mappings", exc)
    except Exception as e:
        return db.failure_message('Update project work items source state mappings', e)


def import_repositories(organization_key, repository_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.import_repositories(
                    session,
                    organization_key,
                    repository_summaries
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Imported Repositories", exc)
    except Exception as e:
        return db.failure_message('', e)


def create_feature_flag(create_feature_flag_input):
    try:
        with db.orm_session() as session:
            return success(
                impl.create_feature_flag(
                    session,
                    create_feature_flag_input.name
                )
            )
    # Adding Integrity error explicitly, to send a clear message to client,  with reason for failure
    except IntegrityError as exc:
        return db.process_exception(f'Feature flag {create_feature_flag_input.name} already exists', exc)
    except SQLAlchemyError as exc:
        return db.process_exception("Create Feature Flag failed", exc)
    except Exception as e:
        return db.failure_message('Create Feature Flag failed', e)


def feature_flag_enablement(feature_flag_enablement_input):
    try:
        with db.orm_session() as session:
            return success(
                impl.feature_flag_enablement(
                    session,
                    feature_flag_enablement_input.feature_flag_key,
                    feature_flag_enablement_input.enablements
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Failed to create feature flag enablement", exc)
    except Exception as e:
        return db.failure_message('Failed to create feature flag enablement', e)

def update_enablements_status(update_enablements_status_input):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_enablements_status(
                    session,
                    update_enablements_status_input.feature_flag_key,
                    update_enablements_status_input.enablements
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Failed to update enablement(s)", exc)
    except Exception as e:
        return db.failure_message(f"Failed to update enablement(s) due to: {e}", e)

def update_feature_flag_status(update_feature_flag_status_input):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_feature_flag_status(
                    session,
                    update_feature_flag_status_input.feature_flag_key,
                    update_feature_flag_status_input.enable_all
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Failed to enable feature flag", exc)
    except Exception as e:
        return db.failure_message(f'Failed to enable feature flag due to: {e}', e)