"""ff_alignment_trends_widgets

Revision ID: 13954158ff1c
Revises: 5f0d7fd35289
Create Date: 2020-08-07 16:30:38.030751

"""
from alembic import op
from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag


# revision identifiers, used by Alembic.
revision = '13954158ff1c'
down_revision = '5f0d7fd35289'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('projects.alignment-trends-widgets', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('projects.alignment-trends-widgets', join_this=session)
    session.commit()