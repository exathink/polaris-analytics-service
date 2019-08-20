"""add_source_id_to_work_items_and_work_items_sources

Revision ID: d8f3f7d6b4aa
Revises: eddaa89b46a7
Create Date: 2019-08-20 15:47:29.565104

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd8f3f7d6b4aa'
down_revision = 'eddaa89b46a7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('source_id', sa.String(), nullable=True), schema='analytics')
    op.add_column('work_items_sources', sa.Column('source_id', sa.String(), nullable=True), schema='analytics')
    op.add_column('work_items_sources', sa.Column('work_items_source_type', sa.String(), nullable=True), schema='analytics')
    # ### end Alembic commands ###


def downgrade():
    op.drop_column('work_items_sources', 'work_items_source_type', schema='analytics')
    op.drop_column('work_items_sources', 'source_id', schema='analytics')
    op.drop_column('work_items', 'source_id', schema='analytics')
    # ### end Alembic commands ###
