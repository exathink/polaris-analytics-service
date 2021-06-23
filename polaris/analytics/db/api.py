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


def resolve_pull_requests_for_new_work_items(organization_key, work_item_source_key, work_item_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.resolve_pull_requests_for_work_items(session, organization_key, work_item_source_key,
                                                          work_item_summaries)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Resolve pull requests for new work_items", exc)
    except Exception as e:
        return db.failure_message('Resolve pull requests for new work_items', e)


def resolve_work_items_for_pull_requests(organization_key, repository_key, pull_request_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.resolve_work_items_for_pull_requests(session, organization_key, repository_key,
                                                          pull_request_summaries)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Resolve work items for pull requests", exc)
    except Exception as e:
        return db.failure_message('Resolve work items for pull requests', e)


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


def move_work_item(organization_key, source_work_item_source_key, target_work_item_source_key, work_item_data):
    try:
        with db.orm_session() as session:
            return success(
                impl.move_work_item(session, source_work_item_source_key, target_work_item_source_key, work_item_data)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("move_work_item failed", exc)
    except Exception as e:
        return db.failure_message('move_work_item failed', e)


def update_contributor(contributor_key, updated_info):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_contributor(
                    session,
                    contributor_key,
                    updated_info
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update contributor for contributor aliases failed", exc)
    except Exception as e:
        return db.failure_message('Update contributor for contributor aliases failed', e)


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


def update_project_work_items(project_work_items):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_project_work_items(
                    session,
                    project_work_items
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update project work items failed", exc)
    except Exception as e:
        return db.failure_message("Update project work items failed", e)


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
        return db.process_exception("Update project work items source state mappings failed", exc)
    except Exception as e:
        return db.failure_message('Update project work items source state mappings failed', e)


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


def import_new_pull_requests(repository_key, pull_request_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.import_new_pull_requests(
                    session,
                    repository_key,
                    pull_request_summaries
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Import new pull requests", exc)
    except Exception as e:
        return db.failure_message('', e)


def update_pull_requests(repository_key, pull_request_summaries):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_pull_requests(
                    session,
                    repository_key,
                    pull_request_summaries
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Update pull requests", exc)
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


def update_feature_flag(update_feature_flag_input):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_feature_flag(
                    session,
                    update_feature_flag_input
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Failed to update feature flag", exc)
    except Exception as e:
        return db.failure_message(f'Failed to update feature flag due to: {e}', e)


def update_project_settings(update_project_settings_input):
    try:
        with db.orm_session() as session:
            return success(
                impl.update_project_settings(
                    session,
                    update_project_settings_input
                )
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Failed to update project settings", exc)
    except Exception as e:
        return db.failure_message(f'Failed to update project settings due to: {e}', e)
