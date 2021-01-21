"""ff_video_guidance

Revision ID: b3595097872f
Revises: d86fcb417bb0
Create Date: 2021-01-21 18:09:46.985637

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = 'b3595097872f'
down_revision = 'd86fcb417bb0'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('system.video_guidance', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('system.video_guidance', join_this=session)
    session.commit()