"""ff_new_card_design

Revision ID: 6781ced0d873
Revises: ebc5bfd19e0b
Create Date: 2022-07-12 17:20:46.618137

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm
from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '6781ced0d873'
down_revision = 'ebc5bfd19e0b'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('ui.new-card-design', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('ui.new-card-design', join_this=session)
    session.commit()