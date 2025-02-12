# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.analytics.db.impl.work_item_resolver import GithubWorkItemResolver, PivotalTrackerWorkItemResolver, \
    JiraWorkItemResolver, GitlabWorkItemResolver, TrelloWorkItemResolver


class TestGithubCommitWorkItemResolution:

    def it_resolves_a_commit_with_a_single_work_item_id(self):
        commit_message = "This fixes issue #2378. No other animals were harmed"

        resolved = GithubWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_commit_with_multiple_work_item_ids(self):
        commit_message = "This fixes issue #2378 and #24532 No other animals were harmed"

        resolved = GithubWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 2
        assert resolved == ['2378', '24532']

    def it_resolves_a_work_item_from_branch_name(self):
        commit_message = "This fixes issue that the branch was created for. No other animals were harmed"

        resolved = GithubWorkItemResolver.resolve(commit_message, branch_name='2378')

        assert len(resolved) == 1
        assert resolved[0] == '2378'


class TestGitlabCommitWorkItemResolution:

    def it_resolves_a_commit_with_a_single_work_item_id(self):
        commit_message = "This fixes issue #2378. No other animals were harmed"

        resolved = GitlabWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_commit_with_multiple_work_item_ids(self):
        commit_message = "This fixes issue #2378 and #24532 No other animals were harmed"

        resolved = GitlabWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 2
        assert resolved == ['2378', '24532']

    def it_resolves_a_work_item_from_branch_name(self):
        commit_message = "This fixes issue that the branch was created for. No other animals were harmed"

        resolved = GitlabWorkItemResolver.resolve(commit_message, branch_name='2378')

        assert len(resolved) == 1
        assert resolved[0] == '2378'


class TestPivotalCommitWorkItemResolution:

    def it_resolves_a_commit_with_a_single_work_item_id(self):
        commit_message = "This fixes [issue #2378]. No other animals were harmed"

        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_commit_with_a_single_work_item_id_preceded_by_a_quote(self):
        commit_message = "This merges branch '2378' into master"

        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_commit_with_multiple_work_item_ids(self):
        commit_message = "This fixes [ issue #2378 and #24532] No other animals were harmed"

        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 2
        assert resolved == ['2378', '24532']

    def it_resolves_a_work_item_from_branch_name(self):
        commit_message = "This fixes issue that the branch was created for. No other animals were harmed"

        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message, branch_name='2378')

        assert len(resolved) == 1
        assert resolved[0] == '2378'

    def it_resolves_a_commit_with_multiple_brackets(self):
        commit_message = "This fixes [ issue #2378 and #24532]. In Addition we broke [ #23167]"

        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 3
        assert resolved == ['2378', '24532', '23167']

    def it_resolves_a_commit_with_no_text_other_than_issue_reference(self):
        commit_message = "This fixes [#24532]."

        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved == ['24532']

    def it_resolves_a_commit_when_there_is_a_newline_in_the_story_mapping_string(self):
        commit_message = """[story=#163732163 subject=work-items-on-commits-view story_url=https://www.pivotaltracker.com/story/show/163732163
        ]"""

        resolved = PivotalTrackerWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved == ['163732163']


class TestJiraWorkItemResolution:

    def it_resolves_a_commit_with_a_single_work_item_id(self):
        commit_message = "This fixes issue MONY-234. No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved[0] == 'MONY-234'

    def it_resolves_a_commit_with_a_numeric_project_key(self):
        commit_message = "This fixes issue E3F-234. No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved[0] == 'E3F-234'

    def it_resolves_a_commits_on_complex_branch_names(self):
        branch_name = "feature/SHOP-731-go-to-today"

        resolved = JiraWorkItemResolver.resolve("test this", branch_name=branch_name)

        assert len(resolved) == 1
        assert resolved[0] == 'SHOP-731'

    def it_resolves_a_work_item_from_branch_name(self):
        commit_message = "This fixes issue. No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(commit_message, branch_name='MONY-234')

        assert len(resolved) == 1
        assert resolved[0] == 'MONY-234'

    def it_resolves_a_commit_with_multiple_work_item_ids(self):
        commit_message = "This fixes issue APPLE-10 and ORANGE-2000 No other animals were harmed"

        resolved = JiraWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 2
        assert resolved == ['APPLE-10', 'ORANGE-2000']

    def it_resolves_a_commit_with_same_work_item_id_multiple_times(self):
        commit_message = "[story=PO-152 subject=jira_work_item_commit_matching story_url=https://urjuna.atlassian.net/browse/PO-152]"

        resolved = JiraWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 2
        assert resolved == ['PO-152', 'PO-152']


class TestTrelloWorkItemResolution:

    def it_resolves_a_commit_with_a_single_work_item_id(self):
        commit_message = "This fixes card #234. No other animals were harmed"

        resolved = TrelloWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved[0] == '234'

    def it_resolves_a_commit_with_different_pattern_work_item_id(self):
        commit_message = "This fixes Card #234. No other animals were harmed"

        resolved = TrelloWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 1
        assert resolved[0] == '234'

    def it_resolves_a_commit_with_multiple_work_item_ids(self):
        commit_message = "This fixes card #10 and card #2000 No other animals were harmed"

        resolved = TrelloWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 2
        assert resolved == ['10', '2000']

    def it_resolves_a_commit_with_same_work_item_id_multiple_times(self):
        commit_message = "[story=#26 subject=trello_work_item_commit_matching story_url=https://trello.com/c/D04wBsLS]"

        resolved = TrelloWorkItemResolver.resolve(commit_message, branch_name='master')

        assert len(resolved) == 2
        assert resolved == ['trello.com/c/D04wBsLS', '26']

    def it_resolves_a_commit_using_branch_name(self):
        commit_message = "This fixes card. No other animals were harmed"

        resolved = TrelloWorkItemResolver.resolve(commit_message, branch_name='234')

        assert len(resolved) == 1
        assert resolved[0] == '234'

    def it_resolves_a_commit_using_branch_name_with_hashtag(self):
        commit_message = "This fixes card. No other animals were harmed"

        resolved = TrelloWorkItemResolver.resolve(commit_message, branch_name='#234')

        assert len(resolved) == 1
        assert resolved[0] == '234'

    def it_resolves_a_commit_with_url_as_branch_name(self):
        commit_message = "This fixes card. No other animals were harmed"

        resolved = TrelloWorkItemResolver.resolve(commit_message, branch_name='trello.com/c/D04wBsLS')

        assert len(resolved) == 1
        assert resolved[0] == 'trello.com/c/D04wBsLS'

    def it_resolves_a_commit_with_identifier_in_message_and_branch_name(self):
        commit_message = "[story=#26 subject=trello_work_item_commit_matching story_url=https://trello.com/c/D04wBsLS]"

        resolved = TrelloWorkItemResolver.resolve(commit_message, branch_name='trello.com/c/D04wBsLS')

        assert len(resolved) == 3
        assert resolved == ['trello.com/c/D04wBsLS', '26', 'trello.com/c/D04wBsLS']
