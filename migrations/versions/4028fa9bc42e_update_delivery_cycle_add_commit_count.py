"""update_delivery_cycle_add_commit_count

Revision ID: 4028fa9bc42e
Revises: 47d55318304d
Create Date: 2020-04-04 08:19:58.471568

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4028fa9bc42e'
down_revision = '47d55318304d'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('commit_count', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'commit_count', schema='analytics')
