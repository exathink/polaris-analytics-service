# -*- coding: utf-8 -*-

from unittest.mock import patch

from graphene.test import Client

from polaris.analytics.service.graphql import schema


class TestCreateWorkItemSourceStateMap:
    def it_creates_a_github_source_state_map(self, setup_schema):
        client = Client(schema)
        with patch('polaris.analytics.publish.work_items_source_state_map_created'):
            response = client.execute("""
                mutation UpdateProjectStateMaps {
                        UpdateProjectStateMaps(
                            UpdateProjectStateMapsInput:{
                                projectKey:"2b10652d-d0c2-4059-a178-060610daef62",
                                workItemsSourceStateMaps: [
                                {
                                    workItemsSourceKey:"afa3c667-f7d5-4806-8af1-94b531c03dc5", 
                                    stateMaps:[
                                    `{
                                            state: "todo",
                                            stateType: "open"
                                        },
                                        {
                                            state: "doing",
                                            stateType:"wip",
                                        },
                                        {
                                            state:"done",
                                            stateType:"complete"
                                        }
                                    ]
                                }
                            ]
                        }){
                            success
                        }
                    }
            """)
            result = response['data']['UpdateProjectStateMaps']
            assert result
            assert result['success']