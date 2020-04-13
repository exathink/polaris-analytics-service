# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.messaging.messages import CommitDetailsCreated


class RegisterSourceFileVersions(CommitDetailsCreated):
    message_type = 'analytics.register_source_file_versions'


class ComputeImplementationComplexityMetricsForCommits(CommitDetailsCreated):
    message_type = 'analytics.compute_implementation_complexity_metrics_for_commits'
