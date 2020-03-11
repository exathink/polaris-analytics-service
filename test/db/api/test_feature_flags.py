# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Maneet Sehgal
import pytest

from polaris.analytics.db import api, model
from polaris.analytics.db.model import FeatureFlag
from polaris.common import db
import uuid
from test.fixtures.graphql import *

test_scope_key = uuid.uuid4()
enablementsInput = [
    dict(scope="user", scope_key=test_scope_key, enabled=True),
    dict(scope="user", scope_key=uuid.uuid4(), enabled=False),
    dict(scope="account", scope_key=uuid.uuid4(), enabled=False)
]


@pytest.yield_fixture()
def create_feature_flag_fixture(cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create("Test Feature Flag")
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def create_feature_flag_enablement_fixture(cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create("New Feature")
        feature_flag.enablements.extend([
            FeatureFlagEnablement(**item)
            for item in enablementsInput
        ])
        session.add(feature_flag)
    yield feature_flag


class objectview(object):
    def __init__(self, d):
        self.__dict__ = d


class TestCreateFeatureFlag:
    def it_creates_feature_flag(self):
        name = 'Test feature flag'
        feature_flag = model.FeatureFlag(
            name=name
        )
        result = api.create_feature_flag(feature_flag)
        assert result['success']
        assert db.connection().execute(
            f"select key from analytics.feature_flags where name='{name}'"
        ).scalar()

    def it_is_not_an_idempotent_create(self):
        name = 'Test feature flag'
        feature_flag = model.FeatureFlag(
            name=name
        )
        api.create_feature_flag(feature_flag)
        # create again.
        result = api.create_feature_flag(feature_flag)
        assert not result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.feature_flags where name='{name}'"
        ).scalar() == 1


class TestUpdateFeatureFlag:
    def it_updates_a_feature_flag(self, create_feature_flag_fixture):
        feature_flag = create_feature_flag_fixture

        feature_flag_enablement_input = dict(
            key=feature_flag.key,
            active=True,
            enable_all=False,
            enablements=enablementsInput
        )

        result = api.update_feature_flag(
            objectview(feature_flag_enablement_input)
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.feature_flag_enablements where feature_flag_id='{feature_flag.id}'"
        ).scalar() == 3

    def it_is_an_idempotent_update(self, create_feature_flag_fixture):
        feature_flag = create_feature_flag_fixture

        feature_flag_enablement_input = dict(
            key=feature_flag.key,
            active=True,
            enable_all=False,
            enablements=enablementsInput
        )

        api.update_feature_flag(
            objectview(feature_flag_enablement_input)
        )
        # update again
        result = api.update_feature_flag(
            objectview(feature_flag_enablement_input)
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.feature_flag_enablements where feature_flag_id='{feature_flag.id}'"
        ).scalar() == 3

    def it_updates_a_feature_flag_enablement(self, create_feature_flag_enablement_fixture):
        feature_flag = create_feature_flag_enablement_fixture

        enablements = [dict(scope="user", scope_key=test_scope_key, enabled=False)]

        feature_flag_enablement_input = dict(
            key=feature_flag.key,
            active=True,
            enable_all=False,
            enablements=enablements
        )

        result = api.update_feature_flag(
            objectview(feature_flag_enablement_input)
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.feature_flag_enablements where feature_flag_id='{feature_flag.id}'"
        ).scalar() == 3
        assert db.connection().execute(
            f"select enabled from analytics.feature_flag_enablements where feature_flag_id='{feature_flag.id}' and scope_key='{test_scope_key}'"
        ).scalar() == False


class TestUpdateFeatureFlagStatus:

    def it_updates_feature_flag_status(self, create_feature_flag_fixture):
        name = 'Test feature flag'
        feature_flag = create_feature_flag_fixture

        update_feature_flag_status_input = dict(
            key=feature_flag.key,
            enable_all=True,
            active=True,
            enablements=None
        )

        result = api.update_feature_flag(objectview(update_feature_flag_status_input))

        assert result['success']
        enabled_status = db.connection().execute(
            f"select enable_all from analytics.feature_flags where key='{feature_flag.key}'"
        ).scalar()
        assert enabled_status == True


class TestDeactivateFeatureFlag:

    def it_deactivates_feature_flag(self, create_feature_flag_fixture):
        name = 'Test feature flag'
        feature_flag = create_feature_flag_fixture

        deactivate_feature_flag_input = dict(
            key=feature_flag.key,
            enable_all=False,
            active=False,
            enablements=None
        )

        result = api.update_feature_flag(objectview(deactivate_feature_flag_input))

        assert result['success']
        active_flag = db.connection().execute(
            f"select active from analytics.feature_flags where key='{feature_flag.key}'"
        ).scalar()
        assert active_flag == False
