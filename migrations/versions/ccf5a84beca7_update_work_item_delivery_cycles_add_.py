"""update_work_item_delivery_cycles_add_merge_commits_code_change_stats_columns

Revision ID: ccf5a84beca7
Revises: a15b1103b02e
Create Date: 2020-04-06 11:28:17.661675

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ccf5a84beca7'
down_revision = 'a15b1103b02e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('average_lines_changed_merge', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('work_item_delivery_cycles', sa.Column('total_files_changed_merge', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('work_item_delivery_cycles', sa.Column('total_lines_changed_merge', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'total_lines_changed_merge', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'total_files_changed_merge', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'average_lines_changed_merge', schema='analytics')
