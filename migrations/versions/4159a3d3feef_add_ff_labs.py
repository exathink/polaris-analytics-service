"""add_ff_labs

Revision ID: 4159a3d3feef
Revises: 7c247d4dc855
Create Date: 2024-04-24 21:08:19.008640

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm
from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '4159a3d3feef'
down_revision = '7c247d4dc855'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('system.labs', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('system.labs', join_this=session)
    session.commit()
