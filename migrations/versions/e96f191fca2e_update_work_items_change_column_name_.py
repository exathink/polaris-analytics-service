"""update_work_items_change_column_name_epic_id_to_parent_id

Revision ID: e96f191fca2e
Revises: c8a96a2142aa
Create Date: 2020-09-24 14:38:13.090894

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e96f191fca2e'
down_revision = 'c8a96a2142aa'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('parent_id', sa.Integer(), nullable=True), schema='analytics')
    op.drop_constraint('work_items_epic_id_fk', 'work_items', schema='analytics', type_='foreignkey')
    op.create_foreign_key('work_items_parent_id_fk', 'work_items', 'work_items', ['parent_id'], ['id'], source_schema='analytics', referent_schema='analytics')
    op.drop_column('work_items', 'epic_id', schema='analytics')


def downgrade():
    op.add_column('work_items', sa.Column('epic_id', sa.INTEGER(), autoincrement=False, nullable=True), schema='analytics')
    op.drop_constraint('work_items_parent_id_fk', 'work_items', schema='analytics', type_='foreignkey')
    op.create_foreign_key('work_items_epic_id_fk', 'work_items', 'work_items', ['epic_id'], ['id'], source_schema='analytics', referent_schema='analytics')
    op.drop_column('work_items', 'parent_id', schema='analytics')
