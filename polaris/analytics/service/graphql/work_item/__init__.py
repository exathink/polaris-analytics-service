from polaris.graphql.selectable import Selectable
from polaris.graphql.selectable import CountableConnection
from polaris.graphql.interfaces import NamedNode
from polaris.analytics.service.graphql.interface_mixins import NamedNodeResolverMixin
from polaris.analytics.service.graphql.work_item.selectable import WorkItemNode


class WorkItem(
    # interface resolver mixins
    NamedNodeResolverMixin,
    # selectable
    Selectable
):
    class Meta:
        interfaces = (NamedNode, )
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