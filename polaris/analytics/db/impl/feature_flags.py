# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

from polaris.analytics.db.model import FeatureFlag, feature_flag_enablements, FeatureFlagEnablement
from polaris.utils.collections import find
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.analytics.db.impl')


def create_feature_flag(session, name):
    logger.info("Inside create_feature_flag")

    feature_flag = FeatureFlag.create(name=name)
    session.add(feature_flag)

    return dict(
        name=name,
        key=feature_flag.key
    )

def feature_flag_enablement(session, feature_flag_key, feature_flag_enablement_input):
    logger.info("Inside feature_flag_enablement")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    if feature_flag is not None:
        enablements = insert(feature_flag_enablements).values([
            dict(
                feature_flag_id=feature_flag.id,
                **enablement
            )
            for enablement in feature_flag_enablement_input
        ])
        inserted = session.connection().execute(
            enablements
        ).rowcount
        return dict(
            imported=inserted
        )
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")


def update_enablements_status(session, feature_flag_key, update_enablements_status_input):
    logger.info(f"Inside update_enablements_status {update_enablements_status_input}")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    logger.info(f'Feature flag {feature_flag.name}')
    updated = []
    if feature_flag is not None:
        for enablement in update_enablements_status_input:
            logger.info(f"Enablement {enablement.scope_key}, {feature_flag.enablements[0].scope_key}")
            if find(feature_flag.enablements, lambda e: str(e.scope_key) == enablement.scope_key):
                updated.append(session.execute(
                    feature_flag_enablements.update().values(
                        enabled=enablement.enabled
                    ).where(
                        and_(
                            feature_flag_enablements.c.scope_key == enablement.scope_key,
                            feature_flag_enablements.c.feature_flag_id == feature_flag.id
                        )
                    )
                ))
            else:
                raise ProcessingException(
                    f"Could not find enablement for scope_key: {enablement.scope_key} associated with feature key: {feature_flag_key}")
        return dict(
            updated=updated
        )
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")


def update_feature_flag_status(session, feature_flag_key, enable_all):
    logger.info("Inside enable_feature_flag")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    if feature_flag is not None:
        feature_flag.enable_all = enable_all
        feature_flag.updated = datetime.utcnow()
        if enable_all:
            feature_flag.enable_all_date = datetime.utcnow()
        else:
            feature_flag.enable_all_date = None
        session.add(feature_flag)
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")

    return dict(
        key=feature_flag_key
    )

def deactivate_feature_flag(session, feature_flag_key):
    logger.info("Inside deactivate_feature_flag")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    if feature_flag is not None:
        feature_flag.active = False
        feature_flag.deactivated_date = datetime.utcnow()
        feature_flag.updated = datetime.utcnow()
        feature_flag.enable_all = False
        feature_flag.enable_all_date = None
        session.add(feature_flag)
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")

    return dict(
        key=feature_flag_key
    )

def create_feature_flag_enablement(session, feature_flag_id, enablement):
    logger.info("Inside create_feature_flag_enablement")
    enablements = insert(feature_flag_enablements).values([
        dict(
            feature_flag_id=feature_flag_id,
            **enablement
        )
    ])
    inserted = session.connection().execute(
        enablements
    ).rowcount
    return dict(
        imported=inserted
    )


def update_enablements(session, feature_flag_key, update_enablements_input):
    logger.info(f"Inside update_enablements_status {update_enablements_input}")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    logger.info(f'Feature flag {feature_flag.name}')
    updated = []
    if feature_flag is not None:
        for enablement in update_enablements_input:
            logger.info(f"Enablement {enablement.scope_key}, {feature_flag.enablements[0].scope_key}")
            if find(feature_flag.enablements, lambda e: str(e.scope_key) == enablement.scope_key):
                updated.append(session.execute(
                    feature_flag_enablements.update().values(
                        enabled=enablement.enabled
                    ).where(
                        and_(
                            feature_flag_enablements.c.scope_key == enablement.scope_key,
                            feature_flag_enablements.c.feature_flag_id == feature_flag.id
                        )
                    )
                ))
            else:
                create_feature_flag_enablement(session, feature_flag.id, enablement)
        return dict(
            updated=updated
        )
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")

def update_feature_flag(session, feature_flag_key, active, enable_all, enablements):
    logger.info("Inside update_feature_flag")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    if feature_flag is not None:
        feature_flag.active = active
        if not active:
            feature_flag.deactivated_date = datetime.now()
            feature_flag.enable_all = False
            feature_flag.enable_all_date = None
        if enable_all:
            feature_flag.enable_all_date = datetime.now()
        else:
            feature_flag.enable_all_date = None
        feature_flag.enable_all = enable_all
        feature_flag.updated = datetime.utcnow()
        update_enablements(session, feature_flag_key, enablements)
        session.add(feature_flag)
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")
    return dict(
        key=feature_flag_key
    )
