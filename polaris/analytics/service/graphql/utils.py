# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


def hash_join(result_rows, join_field, output_type):
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