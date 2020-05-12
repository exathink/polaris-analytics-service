# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from polaris.analytics.messaging.messages import WorkItemsSourceStateMapUpdated
from polaris.messaging.utils import publish
from polaris.messaging.utils import init_topics_to_publish
from polaris.messaging.topics import AnalyticsTopic


def project_work_items_source_state_map_updated(project_state_mappings, channel=None):
    message = WorkItemsSourceStateMapUpdated(
        send=dict(
            project_key=project_state_mappings.project_key,
            work_items_source_state_maps=[
                dict(
                    work_items_source_key=work_items_source.work_items_source_key,
                    state_maps=[
                        dict(
                            state=state_mapping.state,
                            state_type=state_mapping.state
                        )
                        for state_mapping in work_items_source.state_maps
                    ]
                )
                for work_items_source in project_state_mappings.work_items_source_state_maps
            ]
        )
    )
    #init_topics_to_publish(AnalyticsTopic)
    AnalyticsTopic(channel).publish(message)
    # publish(
    #     AnalyticsTopic,
    #     message,
    #     channel=channel
    # )
    return message
