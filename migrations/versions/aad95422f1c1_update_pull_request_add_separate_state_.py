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
    op.alter_column('pull_requests', 'state',
                    existing_type=sa.VARCHAR(),
                    new_column_name='source_state',
                    schema='analytics')
    op.add_column('pull_requests', sa.Column('state', sa.String(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('pull_requests', 'state', schema='analytics')
    op.alter_column('pull_requests', 'source_state',
               existing_type=sa.VARCHAR(),
               new_column_name='state',
               schema='analytics')

