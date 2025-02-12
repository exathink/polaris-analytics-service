# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

from polaris.analytics.messaging.subscribers import CommitsTopicSubscriber, \
    AnalyticsTopicSubscriber, \
    VcsTopicSubscriber, \
    WorkItemsTopicSubscriber

from polaris.common import db
from polaris.messaging.message_consumer import MessageConsumer
from polaris.utils.config import get_config_provider
from polaris.utils.logging import config_logging
from polaris.utils.token_provider import get_token_provider
from polaris.messaging.utils import init_topics_to_publish
from polaris.messaging.topics import AnalyticsTopic, CommandsTopic

logger = logging.getLogger('polaris.analytics.message_listener')


if __name__ == "__main__":
    config_logging()
    config_provider = get_config_provider()

    logger.info('Connecting to polaris db...')
    db.init(config_provider.get('POLARIS_DB_URL'))
    token_provider = get_token_provider()

    init_topics_to_publish(AnalyticsTopic, CommandsTopic)

    MessageConsumer(
        name='polaris.analytics.message_listener',
        topic_subscriber_classes=[
            CommitsTopicSubscriber,
            AnalyticsTopicSubscriber,
            WorkItemsTopicSubscriber,
            VcsTopicSubscriber
        ],
        token_provider=token_provider
    ).start_consuming()






