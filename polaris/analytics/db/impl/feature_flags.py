# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert

from polaris.analytics.db.model import FeatureFlag, feature_flag_enablements
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


def update_feature_flag(session, update_feature_flag_input):
    logger.info("Inside update_feature_flag")
    feature_flag_key = update_feature_flag_input.key
    active = update_feature_flag_input.active
    enable_all = update_feature_flag_input.enable_all
    enablements = update_feature_flag_input.enablements
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    if feature_flag is not None:
        if active is not None:
            feature_flag.active = active
            if not active:
                feature_flag.deactivated_date = datetime.now()
                feature_flag.enable_all = False
                feature_flag.enable_all_date = None
        if enable_all is not None:
            if enable_all:
                feature_flag.enable_all_date = datetime.now()
            else:
                feature_flag.enable_all_date = None
            feature_flag.enable_all = enable_all
        feature_flag.updated = datetime.utcnow()
        if enablements is not None:
            update_enablements(session, feature_flag_key, enablements)
        session.add(feature_flag)
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")
    return dict(
        key=feature_flag_key
    )


def update_enablements(session, feature_flag_key, update_enablements_input):
    logger.info(f"Inside update_enablements_data {update_enablements_input}")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    logger.info(f'Feature flag {feature_flag.name}')
    if feature_flag is not None:
        upsert = insert(feature_flag_enablements).values([
            dict(
                feature_flag_id=feature_flag.id,
                **enablement
            )
            for enablement in update_enablements_input
        ])
        inserted = session.connection().execute(
            upsert.on_conflict_do_update(
                index_elements=['feature_flag_id', 'scope_key'],
                set_=dict(
                    enabled=upsert.excluded.enabled
                )
            )
        ).rowcount
        return dict(
            imported=inserted
        )
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")
