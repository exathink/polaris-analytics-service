"""add_work_items_source_id_to_delivery_cycles

Revision ID: b6018ace1b14
Revises: 31ea781d8e84
Create Date: 2020-06-04 19:15:13.459867

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b6018ace1b14'
down_revision = '31ea781d8e84'
branch_labels = None
depends_on = None


def upgrade():
    # Initially set the column to nullable = true so that we can add the column before migrating
    # the data for existing rows.
    op.add_column('work_item_delivery_cycles', sa.Column('work_items_source_id', sa.Integer(), nullable=True), schema='analytics')
    op.create_index(op.f('ix_analytics_work_item_delivery_cycles_work_items_source_id'), 'work_item_delivery_cycles', ['work_items_source_id'], unique=False, schema='analytics')
    op.create_foreign_key('fk_work_item_delivery_cycles_work_items_source_id', 'work_item_delivery_cycles', 'work_items_sources', ['work_items_source_id'], ['id'], source_schema='analytics', referent_schema='analytics')
    # copy denormalized columns from work_item to work_item_delivery_cycles
    op.execute("""
        update analytics.work_item_delivery_cycles set work_items_source_id = w.work_items_source_id
        from (
            select id, work_items_source_id from analytics.work_items
        ) as w 
        where w.id = work_item_delivery_cycles.work_item_id
    """)
    # set the column to nullable=False
    op.alter_column("work_item_delivery_cycles", "work_items_source_id", nullable=False, schema='analytics')

def downgrade():

    op.drop_constraint('fk_work_item_delivery_cycles_work_items_source_id', 'work_item_delivery_cycles', schema='analytics', type_='foreignkey')
    op.drop_index(op.f('ix_analytics_work_item_delivery_cycles_work_items_source_id'), table_name='work_item_delivery_cycles', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'work_items_source_id', schema='analytics')
