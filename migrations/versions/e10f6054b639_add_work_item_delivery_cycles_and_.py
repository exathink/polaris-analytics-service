"""add_work_item_delivery_cycles_and_durations_tables

Revision ID: e10f6054b639
Revises: da723f3ca403
Create Date: 2020-03-06 03:00:01.997397

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'e10f6054b639'
down_revision = 'da723f3ca403'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('work_item_delivery_cycles',
                    sa.Column('delivery_cycle_id', sa.Integer(), nullable=False),
                    sa.Column('start_seq_no', sa.Integer(), nullable=False),
                    sa.Column('end_seq_no', sa.Integer(), nullable=True),
                    sa.Column('start_date', sa.DateTime(), nullable=False),
                    sa.Column('end_date', sa.DateTime(), nullable=True),
                    sa.Column('lead_time', sa.Integer(), nullable=True),
                    sa.Column('work_item_id', sa.Integer(), nullable=False),
                    sa.ForeignKeyConstraint(['work_item_id'], ['analytics.work_items.id'], ),
                    sa.PrimaryKeyConstraint('delivery_cycle_id'),
                    schema='analytics'
                    )
    op.create_table('work_item_delivery_cycle_durations',
                    sa.Column('state', sa.String(), nullable=False),
                    sa.Column('cumulative_time_in_state', sa.Integer(), nullable=True),
                    sa.Column('delivery_cycle_id', sa.Integer(), nullable=False),
                    sa.ForeignKeyConstraint(['delivery_cycle_id'],
                                            ['analytics.work_item_delivery_cycles.delivery_cycle_id'], ),
                    sa.PrimaryKeyConstraint('state', 'delivery_cycle_id'),
                    schema='analytics'
                    )
    op.add_column('work_items', sa.Column('current_delivery_cycle_id', sa.Integer(), nullable=True), schema='analytics')
    op.create_foreign_key(None, 'work_items', 'work_item_delivery_cycles', ['current_delivery_cycle_id'],
                          ['delivery_cycle_id'], source_schema='analytics', referent_schema='analytics')


def downgrade():
    op.drop_constraint(None, 'work_items', schema='analytics', type_='foreignkey')
    op.drop_column('work_items', 'current_delivery_cycle_id', schema='analytics')
    op.drop_table('work_item_delivery_cycles', schema='analytics')
    op.drop_table('work_item_delivery_cycle_durations', schema='analytics')
