"""update_work_item_delivery_cycles_add_code_change_stats_columns

Revision ID: 13fe1b26ccb9
Revises: 11795bf4a18b
Create Date: 2020-04-02 11:48:52.591876

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '13fe1b26ccb9'
down_revision = '11795bf4a18b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('total_files_changed_non_merge', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('work_item_delivery_cycles', sa.Column('total_lines_changed_non_merge', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('work_item_delivery_cycles', sa.Column('total_lines_deleted_non_merge', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('work_item_delivery_cycles', sa.Column('total_lines_inserted_non_merge', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'total_lines_inserted_non_merge', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'total_lines_deleted_non_merge', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'total_lines_changed_non_merge', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'total_files_changed_non_merge', schema='analytics')
