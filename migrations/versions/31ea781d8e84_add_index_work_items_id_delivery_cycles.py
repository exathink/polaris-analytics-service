"""add_index_work_items_id_delivery_cycles

Revision ID: 31ea781d8e84
Revises: 6ecf5164bae5
Create Date: 2020-06-04 16:20:22.833826

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '31ea781d8e84'
down_revision = '6ecf5164bae5'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(op.f('ix_analytics_work_item_delivery_cycles_work_item_id'), 'work_item_delivery_cycles',
                    ['work_item_id'], unique=False, schema='analytics')


def downgrade():
    op.drop_index(op.f('ix_analytics_work_item_delivery_cycles_work_item_id'), table_name='work_item_delivery_cycles',
                  schema='analytics')
