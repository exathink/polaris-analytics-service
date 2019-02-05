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
