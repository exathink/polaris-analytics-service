# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



from flask_login import current_user
from polaris.common import db
from polaris.analytics.db.model import Account, Organization
import uuid
from datetime import datetime


def create_account(company, owner_key=None       , join_this=None):
    with db.orm_session(join_this) as session:
        organization = Organization(
            key=uuid.uuid4(),
            name=company,
            created=datetime.utcnow()
        )
        account = Account(
            key=uuid.uuid4(),
            name=company,
            owner_key=owner_key,
            created=datetime.utcnow()
        )
        account.organizations.append(organization)
        session.add(account)
        return account
