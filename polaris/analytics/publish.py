# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.messaging.topics import AnalyticsTopic
from polaris.messaging.utils import publish
from polaris.analytics.messaging.messages import ContributorTeamAssignmentsChanged
from polaris.analytics.messaging.commands import ResolveCommitsForWorkItems, RecalculateCycleMetricsForWorkItemSource, ProjectCustomTypeMappingsChanged


def contributor_team_assignments_changed(organization_key, contributor_team_assignments, channel=None):
    message = ContributorTeamAssignmentsChanged(
        send=dict(
            organization_key=organization_key,
            contributor_team_assignments=contributor_team_assignments
        )
    )
    publish(
        AnalyticsTopic,
        message,
        channel=channel
    )
    return message


def resolve_commits_for_work_items(organization_key, work_items_source_key, work_item_summaries, channel=None):
    message = ResolveCommitsForWorkItems(
        send=dict(
            organization_key=organization_key,
            work_items_source_key=work_items_source_key,
            new_work_items=work_item_summaries
        )
    )
    publish(
        AnalyticsTopic,
        message,
        channel=channel
    )
    return message

def recalculate_cycle_metrics_for_work_items_source(project_key, work_items_source_key, rebuild_delivery_cycles,  channel=None):
    message = RecalculateCycleMetricsForWorkItemSource(
        send=dict(
            project_key=project_key,
            work_items_source_key=work_items_source_key,
            rebuild_delivery_cycles=rebuild_delivery_cycles
        )
    )
    publish(
        AnalyticsTopic,
        message,
        channel=channel
    )
    return message

def project_custom_type_mappings_changed(project_key, work_items_source_keys, channel=None):
    message = ProjectCustomTypeMappingsChanged(
        send=dict(
            project_key=project_key,
            work_items_source_keys=work_items_source_keys
        )
    )
    publish(
        AnalyticsTopic,
        message,
        channel=channel
    )
    return message