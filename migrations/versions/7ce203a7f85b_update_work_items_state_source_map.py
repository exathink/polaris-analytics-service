"""update_work_items_state_source_map

Revision ID: 7ce203a7f85b
Revises: 209f6fa05216
Create Date: 2020-03-09 12:35:14.862999

"""
from alembic import op
import sqlalchemy as sa
from polaris.analytics.db.enums import WorkItemsStateType
from polaris.common.enums import WorkTrackingIntegrationType
from sqlalchemy.ext.declarative import declarative_base

# revision identifiers, used by Alembic.
revision = '7ce203a7f85b'
down_revision = '209f6fa05216'
branch_labels = None
depends_on = None


def update_default_state_maps(openStateMapping, completeStateMapping, wipStateMapping):
    #  github
    op.execute(f"""
                update analytics.work_items_source_state_map set state_type='{openStateMapping}' 
                from analytics.work_items_sources
                where state='created'
                and analytics.work_items_sources.id = analytics.work_items_source_state_map.work_items_source_id
                and integration_type='{WorkTrackingIntegrationType.github.value}';
            """)
    op.execute(f"""
                update analytics.work_items_source_state_map set state_type='{completeStateMapping}' 
                from analytics.work_items_sources
                where state='closed'
                and analytics.work_items_sources.id = analytics.work_items_source_state_map.work_items_source_id
                and integration_type='{WorkTrackingIntegrationType.github.value}';
            """)

    # {WorkTrackingIntegrationType.pivotal.value}
    op.execute(f"""
                update analytics.work_items_source_state_map set state_type='{openStateMapping}' 
                from analytics.work_items_sources
                where analytics.work_items_source_state_map.state='created'
                and analytics.work_items_sources.id = analytics.work_items_source_state_map.work_items_source_id
                and analytics.work_items_sources.integration_type='{WorkTrackingIntegrationType.pivotal.value}';
            """)
    op.execute(f"""
                update analytics.work_items_source_state_map set state_type='{completeStateMapping}' 
                from analytics.work_items_sources
                where state='accepted'
                and analytics.work_items_sources.id = analytics.work_items_source_state_map.work_items_source_id
                and integration_type='{WorkTrackingIntegrationType.pivotal.value}';
            """)
    op.execute(f"""
                update analytics.work_items_source_state_map set state_type='{wipStateMapping}' 
                from analytics.work_items_sources
                where state='finished'
                and analytics.work_items_sources.id = analytics.work_items_source_state_map.work_items_source_id
                and integration_type='{WorkTrackingIntegrationType.pivotal.value}';
            """)
    op.execute(f"""
                update analytics.work_items_source_state_map set state_type='{wipStateMapping}' 
                from analytics.work_items_sources
                where state='delivered'
                and analytics.work_items_sources.id = analytics.work_items_source_state_map.work_items_source_id
                and integration_type='{WorkTrackingIntegrationType.pivotal.value}';
            """)

def upgrade():
    # Run the data migration
    update_default_state_maps(WorkItemsStateType.backlog.value, WorkItemsStateType.closed.value, WorkItemsStateType.complete.value)
    update_work_items_state()

def update_work_items_state():
    op.execute("update analytics.work_items set state_type=analytics.work_items_source_state_map.state_type "
               "from analytics.work_items_source_state_map where analytics.work_items.work_items_source_id = analytics.work_items_source_state_map.work_items_source_id "
               "and analytics.work_items.state=analytics.work_items_source_state_map.state")


def downgrade():
    # Run the data migration
    update_default_state_maps(WorkItemsStateType.open.value, WorkItemsStateType.complete.value, WorkItemsStateType.wip.value)
    update_work_items_state()
