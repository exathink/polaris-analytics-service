# -*- coding: utf-8 -*-

from unittest.mock import patch

from graphene.test import Client

from polaris.analytics.service.graphql import schema


class TestUpdateProjectStateMaps:
    def it_creates_a_github_source_state_map(self, setup_schema):
        client = Client(schema)

        response = client.execute("""
            mutation UpdateProjectStateMaps {
                    updateProjectStateMaps(
                        updateProjectStateMapsInput:{
                            projectKey:"2b10652d-d0c2-4059-a178-060610daef62",
                            workItemsSourceStateMaps: [
                            {
                                workItemsSourceKey:"afa3c667-f7d5-4806-8af1-94b531c03dc5", 
                                stateMaps:[
                                    {
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
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']