"""add_columns_is_epic_and_epic_key_to_work_items_table

Revision ID: fae565191c31
Revises: e940baf6688e
Create Date: 2020-08-24 15:34:37.582030

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fae565191c31'
down_revision = 'e940baf6688e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('epic_id', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('work_items', sa.Column('is_epic', sa.Boolean(), server_default='FALSE', nullable=False), schema='analytics')
    op.create_foreign_key('epic_issue_relationship', 'work_items', 'work_items', ['epic_id'], ['id'], source_schema='analytics', referent_schema='analytics')


def downgrade():
    op.drop_constraint('epic_issue_relationship', 'work_items', schema='analytics', type_='foreignkey')
    op.drop_column('work_items', 'is_epic', schema='analytics')
    op.drop_column('work_items', 'epic_id', schema='analytics')
