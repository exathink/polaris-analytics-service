"""add_budget_column_to_work_items_table

Revision ID: fe88b8d6b964
Revises: b3595097872f
Create Date: 2021-03-08 07:58:35.977084

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fe88b8d6b964'
down_revision = 'b3595097872f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('budget', sa.Float(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'budget', schema='analytics')
