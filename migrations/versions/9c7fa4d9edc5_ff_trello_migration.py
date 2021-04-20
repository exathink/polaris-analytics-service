"""ff_trello_migration

Revision ID: 9c7fa4d9edc5
Revises: 2c6b8972d8db
Create Date: 2021-04-19 16:45:22.921153

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag


# revision identifiers, used by Alembic.
revision = '9c7fa4d9edc5'
down_revision = '2c6b8972d8db'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('connectors.trello', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('connectors.trello', join_this=session)
    session.commit()
