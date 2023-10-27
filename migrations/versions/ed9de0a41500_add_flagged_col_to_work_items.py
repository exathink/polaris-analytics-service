"""add_flagged_col_to_work_items

Revision ID: ed9de0a41500
Revises: f835ec1a7a52
Create Date: 2023-10-27 15:44:56.513224

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ed9de0a41500'
down_revision = 'f835ec1a7a52'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('flagged', sa.Boolean(), server_default='FALSE', nullable=True), schema='analytics')


def downgrade():

    op.drop_column('work_items', 'flagged', schema='analytics')
