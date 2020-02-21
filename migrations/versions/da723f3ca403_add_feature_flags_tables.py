"""add_feature_flags_tables

Revision ID: da723f3ca403
Revises: e45064a0ba28
Create Date: 2020-02-19 09:44:28.769579

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'da723f3ca403'
down_revision = 'e45064a0ba28'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('feature_flags',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=256), nullable=False),
    sa.Column('key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('active', sa.Boolean(), server_default=sa.text('TRUE'), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=True),
    sa.Column('enable_all', sa.Boolean(), server_default=sa.text('FALSE'), nullable=False),
    sa.Column('enable_all_date', sa.DateTime(), nullable=True),
    sa.Column('deactivated_date', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('key'),
    sa.UniqueConstraint('name'),
    schema='analytics'
    )
    op.create_table('feature_flag_enablements',
    sa.Column('scope', sa.String(length=256), nullable=False),
    sa.Column('scope_key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('enabled', sa.Boolean(), server_default='FALSE', nullable=False),
    sa.Column('feature_flag_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['feature_flag_id'], ['analytics.feature_flags.id'], ),
    sa.PrimaryKeyConstraint('feature_flag_id', 'scope_key'),
    schema='analytics'
    )


def downgrade():
    op.drop_table('feature_flag_enablements', schema='analytics')
    op.drop_table('feature_flags', schema='analytics')
