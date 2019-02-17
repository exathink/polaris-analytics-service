"""Added work_item_type column to work_items

Revision ID: 7b57db25b731
Revises: 2b8ea08826a7
Create Date: 2019-02-17 14:45:14.846126

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7b57db25b731'
down_revision = '2b8ea08826a7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('work_item_type', sa.String(), nullable=False), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'work_item_type', schema='analytics')

