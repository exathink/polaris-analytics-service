# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text, select, join
import inspect
from polaris.common import db

def properties(clazz):
    return [
        attr[0] for attr in inspect.getmembers(clazz, lambda a: not(inspect.isroutine(a)))
             if not attr[0].startswith('_') and not attr[0].endswith('_')
    ]


def resolve_local_join(result_rows, join_field, output_type):
    if len(result_rows) == 1:
        instances = result_rows[0]
    else:
        instance_hash = {}
        for rows in result_rows:
            for row in rows:
                join_value = row[join_field]
                if join_value is not None:
                    current = instance_hash.get(join_value, None)
                    if current is None:
                        instance_hash[join_value] = {}
                    for key, value in row.items():
                        instance_hash[join_value][key] = value
        instances = instance_hash.values()

    return [output_type(**instance) for instance in instances]



def join_queries(queries, join_field):
    alias = lambda interface: interface.__name__

    if len(queries) > 0:
        # build a list of output columns for the queries
        # list is built by unqualified names reading from left to right on the list of queries.
        # if there are duplicate columns between queries the first one encountered is selected and rest are
        # dropped from the output columns. The resulting set of columns must be a valid
        # set of attributes to pass on to the constructor of output_type.
        seen_columns = set()
        output_columns = []
        for interface, _ in queries:
            for field in properties(interface):
                if field not in seen_columns:
                   seen_columns.add(field)
                   output_columns.append(text(f'{alias(interface)}.{field}'))

        # Convert input pairs (interface, raw-sql) into pairs (table_alias, text(raw-sql) tuples
        # these will be user to construct the final join statement
        subqueries = [
            (alias(interface), text(f"({query}) AS {alias(interface)}"))
            for interface, query in queries
        ]

        # Create the join statement:
        # join statements are of the form subquery[0] left outer join subquery[i] on alias[0].join_field = alias[i].field for i > 0
        # In practice subquery[0] will be the named node generator, so this will contain all the entities in the space,
        # but otherwise it is possible that we return less tha the full set of entities in the space.
        root_alias, selectable = subqueries[0]
        for alias, subquery in subqueries[1:]:
            selectable = join(selectable, subquery, onclause=text(f"{root_alias}.{join_field} = {alias}.{join_field}"), isouter=True)

        # Select the output columns from the resulting join
        return select(output_columns).select_from(selectable)


def resolve_remote_join(queries, output_type, join_field='id', params=None):
    with db.create_session() as session:
        result  = session.execute(join_queries(queries, join_field), params).fetchall()
        return [output_type(**{key:value for key, value in row.items()}) for row in result]


def join_queries_with_cte(resolvers, join_field='id'):
    alias = lambda interface: interface.__name__

    if len(resolvers) > 0:
        # build a list of output columns for the queries
        # list is built by unqualified names reading from left to right on the list of queries.
        # if there are duplicate columns between queries the first one encountered is selected and rest are
        # dropped from the output columns. The resulting set of columns must be a valid
        # set of attributes to pass on to the constructor of output_type.
        seen_columns = {'id', 'name', 'key'}
        output_columns = [text('named_nodes.id'), text('named_nodes.name'), text('named_nodes.key')]
        for resolver in resolvers[1:]:
            for field in properties(resolver.interface):
                if field not in seen_columns:
                    seen_columns.add(field)
                    output_columns.append(text(f'"{alias(resolver.interface)}".{field}'))

        # Convert input pairs (interface, raw-sql) into pairs (table_alias, text(raw-sql) tuples
        # these will be user to construct the final join statement
        subqueries = [
            (alias(resolver.interface), resolver.selectable)
            for resolver in resolvers
        ]


        _, named_nodes_selectable = subqueries[0]
        named_nodes = named_nodes_selectable().cte('named_nodes')

        selectable = named_nodes
        for alias, subquery in subqueries[1:]:
            subselect = subquery(named_nodes)
            alias = subselect.alias(alias)
            selectable = selectable.outerjoin(alias, alias.c[join_field] == named_nodes.c[join_field])

        query = select(output_columns).select_from(selectable)
        print(str(query))
        # Select the output columns from the resulting join
        return query


def resolve_cte_join(queries, output_type, join_field='id', params=None):
    with db.create_session() as session:
        result  = session.execute(join_queries_with_cte(queries, join_field), params).fetchall()
        return [output_type(**{key:value for key, value in row.items()}) for row in result]


class SQlQueryMeasureResolver:
    interface = None
    query = None

    @classmethod
    def metadata(cls):
        return cls.interface, cls.query


class GQLException(Exception):
    pass


class AccessDeniedException(GQLException):
    pass