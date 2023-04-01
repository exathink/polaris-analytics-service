# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from io import StringIO

from polaris.common import db
from polaris.analytics.db.model import Account, Organization, FeatureFlag, FeatureFlagEnablement


def find_account(account_key, join_this=None):
    with db.orm_session(join_this) as session:
        return Account.find_by_account_key(session, account_key)


def find_or_create_account_for_user(user, account_name, organization_name, session_to_join=None):
    with db.orm_session(session_to_join) as session:
        account = find_account(user.account_key, session)
        if account is None:
            name = account_name or user.company or f'{user.first_name} {user.last_name}'
            account = Account(
                name=name,
                key=user.account_key,
            )
            organization = Organization(name=organization_name or account.name, key=uuid.uuid4())
            account.organizations.append(organization)
            session.add(account)

        return account


# Feature flag migration utilities

default_feature_flag_enablements = [
    # ----  staging --------------------

    # Exathink
    dict(scope='account', scope_key="3a0480a3-2eb8-4728-987f-674cbe3cf48c", enabled=True),

    # ------ dev -----------------------


    # Polaris-Dev
    dict(scope='account', scope_key="24347f28-0020-4025-8801-dbc627f9415d", enabled=True)
]


def create_feature_flag_with_default_enablements(feature_flag_name, enablements=default_feature_flag_enablements, join_this=None):
    with db.orm_session(join_this) as session:
        feature_flag = FeatureFlag.create(name=feature_flag_name)
        feature_flag.enablements.extend([
            FeatureFlagEnablement(**enablement) for enablement in enablements
        ])
        session.add(feature_flag)


def delete_feature_flag(feature_flag_name, join_this=None):
    with db.orm_session(join_this) as session:
        feature_flag = FeatureFlag.find_by_name(session, feature_flag_name)
        if feature_flag is not None:
            session.delete(feature_flag)


def literal_postgres_string_array(string_array):
    # TODO:
    # we need this hack due to some obscure type conversions issues in
    # the ancient version of sqlalchemy we are using.
    # revert to using builtin functions when we upgrade
    output = StringIO()
    output.write("{")
    count = 0
    for item in string_array:
        if count > 0:
            output.write(",")

        output.write(f"\"{item}\"")
        count = count + 1

    output.write("}")
    return output.getvalue()