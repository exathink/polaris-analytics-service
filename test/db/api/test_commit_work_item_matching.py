# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.analytics.db.impl.work_item_resolver import GithubWorkItemResolver, PivotalTrackerWorkItemResolver, JiraWorkItemResolver

class TestGithubCommitWorkItemResolution:


    def it_resolves_a_commit_with_a_single_work_item_id(self):
        commit_message = "This fixes issue #2378. No other animals were harmed"


        resolved = GithubWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_commit_with_multiple_work_item_ids(self):
        commit_message = "This fixes issue #2378 and #24532 No other animals were harmed"



        resolved = GithubWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 2
        assert resolved == ['2378', '24532']



class TestPivotalCommitWorkItemResolution:


    def it_resolves_a_commit_with_a_single_work_item_id(self):
        commit_message = "This fixes [issue #2378]. No other animals were harmed"



        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_commit_with_multiple_work_item_ids(self):
        commit_message = "This fixes [ issue #2378 and #24532] No other animals were harmed"


        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 2
        assert resolved == ['2378', '24532']

    def it_resolves_a_commit_with_multiple_brackets(self):
        commit_message = "This fixes [ issue #2378 and #24532]. In Addition we broke [ #23167]"



        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 3
        assert resolved == ['2378', '24532', '23167']

    def it_resolves_a_commit_with_no_text_other_than_issue_reference(self):
        commit_message = "This fixes [#24532]."



        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 1
        assert resolved == ['24532']


    def it_resolves_a_commit_when_there_is_a_newline_in_the_story_mapping_string(self):
        commit_message = """[story=#163732163 subject=work-items-on-commits-view story_url=https://www.pivotaltracker.com/story/show/163732163
        ]"""

        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 1
        assert resolved == ['163732163']


class TestJiraWorkItemResolution:

    def it_resolves_a_commit_with_a_single_work_item_id(self):
        commit_message = "This fixes issue MONY-234. No other animals were harmed"


        resolved = JiraWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 1
        assert resolved[0] == 'MONY-234'

    def it_resolves_a_commit_with_multiple_work_item_ids(self):
        commit_message = "This fixes issue APPLE-10 and ORANGE-2000 No other animals were harmed"



        resolved = JiraWorkItemResolver.resolve(commit_message)

        assert len(resolved) == 2
        assert resolved == ['APPLE-10', 'ORANGE-2000']

