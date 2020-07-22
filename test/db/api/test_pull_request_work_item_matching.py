# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from polaris.analytics.db.impl.work_item_resolver import GithubWorkItemResolver, PivotalTrackerWorkItemResolver, \
    JiraWorkItemResolver


class TestGithubPullRequestWorkItemResolution:

    def it_resolves_a_pull_request_with_a_single_work_item_id(self):
        title = "This fixes issue #2378. No other animals were harmed"
        description = "Resolves #2378. No other animals were harmed"

        resolved = GithubWorkItemResolver.resolve(title, description, branch_name='#2378')

        assert len(resolved) == 3
        assert resolved[0] == '2378'

    def it_resolves_a_pull_request_with_multiple_work_item_ids(self):
        title = "This fixes issue #2378 and #24532 "
        description = "Resolves #2378 and #24532. No other animals were harmed"

        resolved = GithubWorkItemResolver.resolve(title, description, branch_name='PO-2378-PO-24532')

        assert len(resolved) == 4
        assert set(resolved) == {'2378', '24532'}

    def it_resolves_a_work_item_from_branch_name(self):
        title = "This fixes issue that the branch was created for. No other animals were harmed"

        resolved = GithubWorkItemResolver.resolve(title, branch_name='2378')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_work_item_from_title(self):
        title = "This fixes issue #2378. No other animals were harmed"
        description = ''

        resolved = GithubWorkItemResolver.resolve(title, branch_name='random')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_work_item_from_description(self):
        title = ''
        description = "This fixes issue #2378. No other animals were harmed"

        resolved = GithubWorkItemResolver.resolve(description, branch_name='random')

        assert len(resolved) == 1
        assert resolved[0] == '2378'


class TestPivotalPullRequestWorkItemResolution:

    def it_resolves_a_pull_request_with_a_single_work_item_id(self):
        title = "This fixes [issue #2378]. No other animals were harmed"
        description = "This fixes [issue #2378]. No other animals were harmed"

        resolved = PivotalTrackerWorkItemResolver.resolve(title, description, branch_name='#2378')

        assert len(resolved) == 3
        assert resolved[0] == '2378'

    def it_resolves_a_pull_request_with_a_single_work_item_id_preceded_by_a_quote(self):
        title = "This merges branch '2378' into master"
        description = "This merges branch into master"

        resolved = PivotalTrackerWorkItemResolver.resolve(title, description, branch_name='random')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_pull_request_with_multiple_work_item_ids(self):
        title = "This fixes [ issue #2378 and #24532] No other animals were harmed"
        description = ""

        resolved = PivotalTrackerWorkItemResolver.resolve(title, description, branch_name='#2378')

        assert len(resolved) == 3
        assert resolved == ['2378', '24532', '2378']

    def it_resolves_a_work_item_from_branch_name(self):
        title = "This fixes issue that the branch was created for. No other animals were harmed"

        resolved = PivotalTrackerWorkItemResolver.resolve(title, '', branch_name='2378')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_work_item_from_title(self):
        title = "This fixes issue #2378. No other animals were harmed"
        description = ''

        resolved = PivotalTrackerWorkItemResolver.resolve(title, description, branch_name='random')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_work_item_from_description(self):
        title = ''
        description = "This fixes issue #2378. No other animals were harmed"

        resolved = PivotalTrackerWorkItemResolver.resolve(title, description, branch_name='random')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_pull_request_with_multiple_brackets(self):
        title = "This fixes [ issue #2378 and #24532]. In Addition we broke [ #23167]"

        resolved = PivotalTrackerWorkItemResolver.resolve(title, branch_name='random')

        assert len(resolved) == 3
        assert resolved == ['2378', '24532', '23167']

    def it_resolves_a_pull_request_with_no_text_other_than_issue_reference(self):
        title = "[#24532]."

        resolved = PivotalTrackerWorkItemResolver.resolve(title, branch_name='master')

        assert len(resolved) == 1
        assert resolved == ['24532']

    def it_resolves_a_pull_request_when_there_is_a_newline_in_the_story_mapping_string(self):
        description = """[story=#163732163 subject=work-items-on-commits-view story_url=https://www.pivotaltracker.com/story/show/163732163
        ]"""

        resolved = PivotalTrackerWorkItemResolver.resolve(description, branch_name='master')

        assert len(resolved) == 1
        assert resolved == ['163732163']


class TestJiraPullRequestWorkItemResolution:

    def it_resolves_a_pull_request_with_a_single_work_item_id(self):
        title = "This fixes issue MONY-234. No other animals were harmed"
        description = "This fixes issue MONY-234. No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(title, description, branch_name='MONY-234')

        assert len(resolved) == 3
        assert resolved[0] == 'MONY-234'

    def it_resolves_a_pull_request_with_a_numeric_project_key(self):
        title = "This fixes issue E3F-234. No other animals were harmed"
        description = "This fixes issue E3F-234. No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(title, description, branch_name='E3F-234')

        assert len(resolved) == 3
        assert resolved[0] == 'E3F-234'

    def it_resolves_a_pull_request_on_complex_branch_names(self):
        branch_name = "feature/SHOP-731-go-to-today"

        resolved = JiraWorkItemResolver.resolve("test this", "test this", branch_name=branch_name)

        assert len(resolved) == 1
        assert resolved[0] == 'SHOP-731'

    def it_resolves_a_work_item_from_branch_name(self):
        title = "This fixes issue. No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(title, '', branch_name='MONY-234')

        assert len(resolved) == 1
        assert resolved[0] == 'MONY-234'

    def it_resolves_a_work_item_from_title(self):
        title = "This fixes issue MONY-234. No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(title, '', branch_name='random')

        assert len(resolved) == 1
        assert resolved[0] == 'MONY-234'

    def it_resolves_a_work_item_from_description(self):
        title = ''
        description = "This fixes issue MONY-234. No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(title, description, branch_name='MONY-random')

        assert len(resolved) == 1
        assert resolved[0] == 'MONY-234'

    def it_resolves_a_pull_request_with_multiple_work_item_ids(self):
        title = "This fixes issue APPLE-10 and ORANGE-2000 No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(title, '', branch_name='APPLE-10/ORANGE-2000')

        assert len(resolved) == 4
        assert resolved == ['APPLE-10', 'ORANGE-2000', 'APPLE-10', 'ORANGE-2000']

    def it_resolves_a_pull_request_with_same_work_item_id_multiple_times(self):
        title = "[story=PO-152 subject=jira_work_item_commit_matching story_url=https://urjuna.atlassian.net/browse/PO-152]"
        description = ""

        resolved = JiraWorkItemResolver.resolve(title, description, branch_name='PO-152')

        assert len(resolved) == 3
        assert resolved == ['PO-152', 'PO-152', 'PO-152']
