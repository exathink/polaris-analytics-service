# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

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


