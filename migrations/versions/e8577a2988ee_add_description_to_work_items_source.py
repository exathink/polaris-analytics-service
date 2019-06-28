"""add_description_to_work_items_source

Revision ID: e8577a2988ee
Revises: dddf02c9b763
Create Date: 2019-06-28 19:58:11.785309

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e8577a2988ee'
down_revision = 'dddf02c9b763'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items_sources', sa.Column('description', sa.String(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items_sources', 'description', schema='analytics')
