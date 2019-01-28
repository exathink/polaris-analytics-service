# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import polaris.analytics.db.impl.source_file_import
from polaris.common import db
from polaris.analytics.db import impl
from sqlalchemy.exc import SQLAlchemyError

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
        return db.process_exception("Import new_work_items failed", exc)
    except Exception as e:
        return db.failure_message('Import new_work_items failed', e)

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
                impl.resolve_commits_for_work_items(session, organization_key, work_item_source_key, work_item_summaries)
            )
    except SQLAlchemyError as exc:
        return db.process_exception("Import new_work_items failed", exc)
    except Exception as e:
        return db.failure_message('Import new_work_items failed', e)