"""update_state_type_for_existing_work_items

Revision ID: e45064a0ba28
Revises: 981b74c1f9a4
Create Date: 2020-02-04 07:40:02.255832

"""
from alembic import op
import sqlalchemy as sa
from polaris.analytics.db.enums import WorkItemsStateType


# revision identifiers, used by Alembic.
revision = 'e45064a0ba28'
down_revision = '981b74c1f9a4'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("update analytics.work_items set state_type=analytics.work_items_source_state_map.state_type "
               "from analytics.work_items_source_state_map where analytics.work_items.work_items_source_id = analytics.work_items_source_state_map.work_items_source_id "
               "and analytics.work_items.state=analytics.work_items_source_state_map.state")

def downgrade():
    op.execute("update analytics.work_items set state_type=null")
