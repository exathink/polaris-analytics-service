"""remove_vendor_add_integration_type_description_to_repositories

Revision ID: eddaa89b46a7
Revises: 2026005235f1
Create Date: 2019-07-29 20:25:21.751578

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'eddaa89b46a7'
down_revision = '2026005235f1'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('repositories', sa.Column('description', sa.Text(), nullable=True), schema='analytics')
    op.add_column('repositories', sa.Column('integration_type', sa.String(), nullable=True), schema='analytics')
    op.drop_column('repositories', 'vendor', schema='analytics')
    # ### end Alembic commands ###


def downgrade():
    op.add_column('repositories', sa.Column('vendor', sa.VARCHAR(length=5), autoincrement=False, nullable=True), schema='analytics')
    op.drop_column('repositories', 'integration_type', schema='analytics')
    op.drop_column('repositories', 'description', schema='analytics')
    # ### end Alembic commands ###
