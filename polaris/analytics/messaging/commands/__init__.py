from polaris.messaging.message_factory import register_messages
from .resolve_commits_work_items import ResolveWorkItemsForCommits
from .on_work_items_commits_resolved import UpdateCommitsWorkItemsSummaries, InferProjectsRepositoriesRelationships, \
    UpdateWorkItemsCommitsStats, ComputeImplementationComplexityMetricsForWorkItems, ComputeContributorMetricsForWorkItems, \
    PopulateWorkItemSourceFileChangesForWorkItems, ResolveTeamsForWorkItems

from .on_commit_details_created import RegisterSourceFileVersions, ComputeImplementationComplexityMetricsForCommits, \
    ComputeContributorMetricsForCommits, PopulateWorkItemSourceFileChangesForCommits
from .on_work_items_created import ResolveCommitsForWorkItems, ResolvePullRequestsForWorkItems
from .on_pull_requests_created import ResolveWorkItemsForPullRequests

from .resolve_work_items_sources_for_repositories import ResolveWorkItemsSourcesForRepositories
from .recalculate_cycle_metrics_for_work_items_source import RecalculateCycleMetricsForWorkItemSource

from .on_project_custom_type_mapping_changed import ProjectCustomTypeMappingsChanged

__exported__ = [
    ResolveWorkItemsForCommits,
    UpdateCommitsWorkItemsSummaries,
    InferProjectsRepositoriesRelationships,
    ResolveWorkItemsSourcesForRepositories,
    UpdateWorkItemsCommitsStats,
    ComputeImplementationComplexityMetricsForWorkItems,
    RegisterSourceFileVersions,
    ComputeImplementationComplexityMetricsForCommits,
    ComputeContributorMetricsForWorkItems,
    ComputeContributorMetricsForCommits,
    PopulateWorkItemSourceFileChangesForCommits,
    PopulateWorkItemSourceFileChangesForWorkItems,
    ResolveCommitsForWorkItems,
    ResolvePullRequestsForWorkItems,
    ResolveWorkItemsForPullRequests,
    ResolveTeamsForWorkItems,
    RecalculateCycleMetricsForWorkItemSource,
    ProjectCustomTypeMappingsChanged
]

register_messages(__exported__)

__all__ = [
    export.__name__ for export in __exported__
]