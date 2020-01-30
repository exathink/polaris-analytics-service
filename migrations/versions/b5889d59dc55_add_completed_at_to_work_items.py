"""add_completed_at_to_work_items

Revision ID: b5889d59dc55
Revises: 11506ba01d30
Create Date: 2020-01-30 21:36:47.037199

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b5889d59dc55'
down_revision = '11506ba01d30'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('completed_at', sa.DateTime(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'completed_at', schema='analytics')
