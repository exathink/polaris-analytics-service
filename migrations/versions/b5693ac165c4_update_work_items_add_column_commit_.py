"""update_work_items_add_column_commit_identifiers

Revision ID: b5693ac165c4
Revises: 9c7fa4d9edc5
Create Date: 2021-04-20 12:27:15.421549

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b5693ac165c4'
down_revision = '9c7fa4d9edc5'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('commit_identifiers', postgresql.JSONB(astext_type=sa.Text()), server_default='[]', nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'commit_identifiers', schema='analytics')
