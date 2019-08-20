"""add_source_id_to_repositories

Revision ID: 26d2096582bb
Revises: d8f3f7d6b4aa
Create Date: 2019-08-20 18:48:04.108587

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '26d2096582bb'
down_revision = 'd8f3f7d6b4aa'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('repositories', sa.Column('source_id', sa.String(), nullable=True), schema='analytics')
    # ### end Alembic commands ###


def downgrade():

    op.drop_column('repositories', 'source_id', schema='analytics')
    # ### end Alembic commands ###
