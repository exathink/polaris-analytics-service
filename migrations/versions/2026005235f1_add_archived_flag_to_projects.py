"""add_archived_flag_to_projects

Revision ID: 2026005235f1
Revises: e8577a2988ee
Create Date: 2019-07-14 16:52:10.192251

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2026005235f1'
down_revision = 'e8577a2988ee'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('projects', sa.Column('archived', sa.Boolean(), server_default=sa.text('FALSE'), nullable=False), schema='analytics')
    # ### end Alembic commands ###


def downgrade():

    op.drop_column('projects', 'archived', schema='analytics')
    # ### end Alembic commands ###
