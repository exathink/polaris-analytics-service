"""ff_value_stream_config

Revision ID: d86fcb417bb0
Revises: a13fdac72101
Create Date: 2021-01-04 19:34:12.595930

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag


# revision identifiers, used by Alembic.
revision = 'd86fcb417bb0'
down_revision = 'a13fdac72101'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('projects.value_stream_config', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('projects.value_stream_config', join_this=session)
    session.commit()
