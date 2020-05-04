# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.analytics.db import commands
from test.fixtures.work_item_source_files import *


class TestPopulateSourceFileChangesForCommits:

    def it_populates_source_file_changes_for_one_commit_one_delivey_cycle_one_contributor(self, work_items_commits_source_files_fixture):
        organization, work_items_ids, test_commits, test_work_items, test_source_files, contributor_list = work_items_commits_source_files_fixture
        commit_details = [
            dict(
                key=test_commits[4]['key']
            )
        ]
        result = commands.populate_work_items_source_file_changes_for_commits(organization.key, commit_details)
        assert result['success']
        assert result['updated'] == 2
        assert db.connection().execute("select count(id) from analytics.work_item_source_file_changes").scalar() == 2
