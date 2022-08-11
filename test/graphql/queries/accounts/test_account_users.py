from unittest.mock import patch

from graphene.test import Client

from polaris.analytics.service.graphql import schema
from polaris.auth.db.model import Role
from polaris.utils.collections import Fixture, lists_are_same
from test.fixtures.users import *


@pytest.mark.skip
class TestAccountUsers:

    @pytest.fixture()
    def setup(self, account_org_user_fixture):
        account, organizations, user = account_org_user_fixture

        yield Fixture(
            account=account,
            organizations=organizations,
            user=user
        )

    class TestUserInfo:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup
            query = """
                            query getAccountUserRoles($account_key: String!) {
                              account(key: $account_key) {
                                users(interfaces: [UserInfo]) {
                                  edges {
                                    node {
                                      id
                                      name
                                      key
                                      firstName
                                      lastName
                                      email
                                    }
                                  }
                                }
                              }
                            }
                    """

            yield Fixture(
                parent=fixture,
                query=query
            )
        def it_returns_the_user_info_fields(self, setup):
            fixture = setup

            client = Client(schema)
            with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
                response = client.execute(fixture.query, variable_values=dict(account_key=fixture.account.key))

            assert 'errors' not in response
            account = response['data']['account']
            assert len(account['users']) == 1
            user = account['users']['edges'][0]['node']

            assert user['name'] == "Emma Woodhouse"
            assert user['email'] == fixture.user.email
            assert user['firstName'] == fixture.user.first_name
            assert user['lastName'] == fixture.user.last_name
            assert user['key'] == fixture.user.key



    class TestUserRoles:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup
            query = """
                    query getAccountUserRoles($account_key: String!) {
                      account(key: $account_key) {
                        users(interfaces: [UserRoles]) {
                          edges {
                            node {
                              organizationRoles {
                                name
                                scopeKey
                                role
                              }
                              accountRoles {
                                name
                                scopeKey
                                role
                              }
                              systemRoles
                            }
                          }
                        }
                      }
                    }
            """


            yield Fixture(
                parent=fixture,
                query=query
            )

        class TestNonAdminUsers:
            def it_returns_user_roles_correctly(self, setup):
                fixture = setup

                client = Client(schema)
                with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
                    response = client.execute(fixture.query, variable_values=dict(account_key=fixture.account.key))

                assert 'errors' not in response
                account = response['data']['account']
                assert len(account['users']) == 1
                user = account['users']['edges'][0]['node']
                assert lists_are_same(
                    user['organizationRoles'],
                    [
                        dict(name=organization.name, scopeKey=organization.key, role='member')
                        for organization in fixture.organizations
                    ]
                )

                assert lists_are_same(
                    user['accountRoles'],
                    [
                        dict(name=fixture.account.name, scopeKey=fixture.account.key, role='owner')
                    ]
                )

                assert user['systemRoles'] == []

        class TestAdminUsers:
            @pytest.fixture()
            def setup(self, setup):
                fixture = setup
                user = fixture.user

                with db.orm_session() as session:
                    session.add(fixture.user)
                    admin_role = Role(name="admin")
                    session.add(admin_role)
                    user.roles.append(admin_role)

                yield fixture

            def it_returns_admin_user_roles_correctly(self, setup):
                fixture = setup

                client = Client(schema)
                with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
                    response = client.execute(fixture.query, variable_values=dict(account_key=fixture.account.key))

                assert 'errors' not in response
                account = response['data']['account']
                assert len(account['users']) == 1
                user = account['users']['edges'][0]['node']
                assert lists_are_same(
                    user['organizationRoles'],
                    [
                        dict(name=organization.name, scopeKey=organization.key, role='member')
                        for organization in fixture.organizations
                    ]
                )

                assert lists_are_same(
                    user['accountRoles'],
                    [
                        dict(name=fixture.account.name, scopeKey=fixture.account.key, role='owner')
                    ]
                )

                assert user['systemRoles'] == ["admin"]

    class TestOrgRolesParams:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup
            # Create a second account, add an org to the account
            # make the user a member of that org.
            # that org role should not show up when
            # we query the org roles for that user from the first account
            with db.orm_session() as session:
                account_2 = Account(key=str(uuid.uuid4()),
                              name='test-account-2')

                organization_3 = Organization(
                    key=str(uuid.uuid4()),
                    name='test-org3',
                    public=False
                )
                account_2.organizations.append(organization_3)
                account_2.add_member(fixture.user, AccountRoles.member)
                organization_3.add_member(fixture.user, OrganizationRoles.owner)
                session.add(account_2)

            query = """
                    query getAccountUserRoles($account_key: String!) {
                      account(key: $account_key) {
                        users(interfaces: [UserRoles], userRolesArgs:{
                        accountKey: $account_key
                        } ) {
                          edges {
                            node {
                              organizationRoles {
                                name
                                scopeKey
                                role
                              }
                              accountRoles {
                                name
                                scopeKey
                                role
                              }
                              systemRoles
                            }
                          }
                        }
                      }
                    }
            """


            yield Fixture(
                parent=fixture,
                query=query,
                account_2=account_2,
                organization_3=organization_3
            )

        def it_returns_the_correct_roles_for_the_user_in_account_2(self, setup):
            fixture = setup

            account_2 = fixture.account_2
            organization_3 = fixture.organization_3

            client = Client(schema)
            with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
                response = client.execute(fixture.query, variable_values=dict(account_key=account_2.key))

            assert 'errors' not in response
            account = response['data']['account']
            assert len(account['users']) == 1
            user = account['users']['edges'][0]['node']
            assert lists_are_same(
                user['organizationRoles'],
                [
                    dict(name=organization_3.name, scopeKey=organization_3.key, role='owner')

                ]
            )

            assert lists_are_same(
                user['accountRoles'],
                [
                    dict(name=account_2.name, scopeKey=account_2.key, role='member')
                ]
            )

            assert user['systemRoles'] == []

        def it_returns_the_correct_roles_for_the_user_in_account_1(self, setup):
            fixture = setup

            client = Client(schema)
            with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
                response = client.execute(fixture.query, variable_values=dict(account_key=fixture.account.key))

            assert 'errors' not in response
            account = response['data']['account']
            assert len(account['users']) == 1
            user = account['users']['edges'][0]['node']
            assert lists_are_same(
                user['organizationRoles'],
                [
                    dict(name=organization.name, scopeKey=organization.key, role='member')
                    for organization in fixture.organizations
                ]
            )

            assert lists_are_same(
                user['accountRoles'],
                [
                    dict(name=fixture.account.name, scopeKey=fixture.account.key, role='owner')
                ]
            )

            assert user['systemRoles'] == []