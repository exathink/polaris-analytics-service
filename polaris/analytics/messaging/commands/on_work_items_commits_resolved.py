# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.messaging.messages import WorkItemsCommitsResolved


class UpdateCommitsWorkItemsSummaries(WorkItemsCommitsResolved):
    message_type = 'analytics.update_commit_work_items_summaries'

