"""add_parent_key_to_work_items

Revision ID: 16c3ad41c116
Revises: 7b620b806cf7
Create Date: 2023-05-31 16:39:58.567520

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '16c3ad41c116'
down_revision = '7b620b806cf7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('parent_key', postgresql.UUID(as_uuid=True), nullable=True), schema='analytics')

    op.execute(
        f"""
        with work_items_parents as (
            select work_items.id, parents.key as parent_key  from analytics.work_items 
            inner join analytics.work_items as parents on work_items.parent_id = parents.id 
            inner join analytics.work_items_sources on work_items.work_items_source_id = work_items_sources.id
            inner join analytics.organizations on work_items_sources.organization_id = organizations.id
            where organizations.key in (
                '52e0eff5-7b32-4150-a1c4-0f55d974ee2a',
                '96e4ebf0-f7fc-44c2-826d-73a027d5aa4d',
                '9860e34a-f69b-478f-b02d-5c1702aab9e1',
                'b63b82d6-ff0e-4c29-a82b-b18e2fa1e1ea',
                '023e4bb5-e284-4d78-85f4-1fc56b60fa51',
                'c6501195-e9fa-49d4-8c36-fbc73a9a4b6f'
            )
        )
        update analytics.work_items set parent_key = work_items_parents.parent_key from work_items_parents where work_items.id = work_items_parents.id
        """
    )


def downgrade():
    op.drop_column('work_items', 'parent_key', schema='analytics')

