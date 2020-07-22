# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.messaging.messages import PullRequestsCreated


class ResolveWorkItemsForPullRequests(PullRequestsCreated):
    message_type = 'analytics.resolve_work_items_for_pull_requests'



