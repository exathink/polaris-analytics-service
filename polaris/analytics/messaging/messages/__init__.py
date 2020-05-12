# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from polaris.messaging.messages import register_messages
from .work_items_events import WorkItemsSourceStateMapUpdated


__exported__ = [
    WorkItemsSourceStateMapUpdated
]

__all__ = [
    export.__name__ for export in __exported__
]

# Add this to the global message factory so that the messages can be desrialized on reciept.
register_messages(__exported__)

