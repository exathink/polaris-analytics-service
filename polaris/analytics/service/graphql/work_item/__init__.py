from polaris.graphql.selectable import Selectable
from polaris.graphql.selectable import CountableConnection
from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemsSourceRef
from polaris.graphql.selectable import ConnectionResolverMixin
from polaris.analytics.service.graphql.interface_mixins import KeyIdResolverMixin, WorkItemInfoResolverMixin
from polaris.analytics.service.graphql.work_item.selectable import WorkItemNode


class WorkItem(
    # interface resolver mixins
    WorkItemInfoResolverMixin,
    # selectable
    Selectable
):
    class Meta:
        interfaces = (WorkItemInfo, WorkItemsSourceRef)
        named_node_resolver = WorkItemNode
        interface_resolvers = {}
        connection_class = lambda: WorkItems



    @classmethod
    def resolve_field(cls, parent, info, key, **kwargs):
        return cls.resolve_instance(key=key, **kwargs)


class WorkItems(
    CountableConnection
):
    class Meta:
        node = WorkItem


class WorkItemsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):

    work_items = WorkItem.ConnectionField()

    def resolve_work_items(self, info, **kwargs):
        return WorkItem.resolve_connection(
            self.get_connection_resolver_context('work_items'),
            self.get_connection_node_resolver('work_items'),
            self.get_instance_query_params(),
            **kwargs
        )