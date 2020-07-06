# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.messaging.messages import WorkItemsCreated


class ResolveCommitsForWorkItems(WorkItemsCreated):
    message_type = 'analytics.resolve_commits_for_work_items'


class ResolvePullRequestsForWorkItems(WorkItemsCreated):
    message_type = 'analytics.resolve_pull_requests_for_work_items'


