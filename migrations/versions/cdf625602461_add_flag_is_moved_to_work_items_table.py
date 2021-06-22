"""add_flag_is_moved_to_work_items_table

Revision ID: cdf625602461
Revises: 9ab1715c3b9f
Create Date: 2021-06-22 11:12:16.548365

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cdf625602461'
down_revision = '9ab1715c3b9f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('is_moved', sa.Boolean(), server_default='FALSE', nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'is_moved', schema='analytics')
