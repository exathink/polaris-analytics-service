"""added work_items_summary column to commits

Revision ID: 2ceb7dbd6e1b
Revises: aebfde4cadfc
Create Date: 2019-02-05 21:21:55.563258

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '2ceb7dbd6e1b'
down_revision = 'aebfde4cadfc'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('commits', sa.Column('work_items_summaries', postgresql.JSONB(astext_type=sa.Text()), server_default='[]', nullable=True), schema='analytics')


def downgrade():
    op.drop_column('commits', 'work_items_summaries', schema='analytics')
