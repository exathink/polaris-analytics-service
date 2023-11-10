"""add_changelog_col_to_work_items

Revision ID: 12180750d51d
Revises: 1c38706b87b4
Create Date: 2023-11-10 20:39:06.714409

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '12180750d51d'
down_revision = '1c38706b87b4'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('work_items', sa.Column('changelog', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=True), schema='analytics')



def downgrade():

    op.drop_column('work_items', 'changelog', schema='analytics')

