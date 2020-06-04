"""add_fk_indexes_for_delivery_cycles

Revision ID: 254d4cad11db
Revises: b6018ace1b14
Create Date: 2020-06-04 22:20:25.373400

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '254d4cad11db'
down_revision = 'b6018ace1b14'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(op.f('ix_analytics_work_item_delivery_cycle_contributors_delivery_cycle_id'),
                    'work_item_delivery_cycle_contributors', ['delivery_cycle_id'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_work_item_delivery_cycle_durations_delivery_cycle_id'),
                    'work_item_delivery_cycle_durations', ['delivery_cycle_id'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_work_item_source_file_changes_delivery_cycle_id'),
                    'work_item_source_file_changes', ['delivery_cycle_id'], unique=False, schema='analytics')


def downgrade():
    op.drop_index(op.f('ix_analytics_work_item_source_file_changes_delivery_cycle_id'),
                  table_name='work_item_source_file_changes', schema='analytics')
    op.drop_index(op.f('ix_analytics_work_item_delivery_cycle_durations_delivery_cycle_id'),
                  table_name='work_item_delivery_cycle_durations', schema='analytics')
    op.drop_index(op.f('ix_analytics_work_item_delivery_cycle_contributors_delivery_cycle_id'),
                  table_name='work_item_delivery_cycle_contributors', schema='analytics')
