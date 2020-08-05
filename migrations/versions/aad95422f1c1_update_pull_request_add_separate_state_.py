"""update_pull_request_add_separate_state_field

Revision ID: aad95422f1c1
Revises: f1656bdab6ba
Create Date: 2020-08-05 06:23:32.495767

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aad95422f1c1'
down_revision = 'f1656bdab6ba'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('pull_requests', sa.Column('source_state', sa.String(), nullable=True), schema='analytics')
    op.alter_column('pull_requests', 'state',
               existing_type=sa.VARCHAR(),
               nullable=True,
               schema='analytics')


def downgrade():
    op.alter_column('pull_requests', 'state',
               existing_type=sa.VARCHAR(),
               nullable=False,
               schema='analytics')
    op.drop_column('pull_requests', 'source_state', schema='analytics')
