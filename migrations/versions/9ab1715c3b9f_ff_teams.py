"""ff_teams

Revision ID: 9ab1715c3b9f
Revises: fd63c9f125b3
Create Date: 2021-06-19 19:41:28.793538

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag


# revision identifiers, used by Alembic.
revision = '9ab1715c3b9f'
down_revision = 'fd63c9f125b3'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('system.teams', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('system.teams', join_this=session)
    session.commit()