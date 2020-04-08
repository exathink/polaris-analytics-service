"""update_delivery_cycle_add_repository_count

Revision ID: 89d03a08133f
Revises: 60f21767bab9
Create Date: 2020-03-31 11:39:40.337806

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '89d03a08133f'
down_revision = '60f21767bab9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('repository_count', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'repository_count', schema='analytics')
