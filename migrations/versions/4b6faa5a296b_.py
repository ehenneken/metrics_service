"""empty message

Revision ID: 4b6faa5a296b
Revises: None
Create Date: 2015-05-14 13:02:51.893908

"""

# revision identifiers, used by Alembic.
revision = '4b6faa5a296b'
down_revision = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_metrics_bibcode', table_name='metrics')
    op.create_index(op.f('ix_metrics_bibcode'), 'metrics', ['bibcode'], unique=False)
    op.drop_column('metrics', 'rn_citations_hist')
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('metrics', sa.Column('rn_citations_hist', postgresql.JSON(), autoincrement=False, nullable=True))
    op.drop_index(op.f('ix_metrics_bibcode'), table_name='metrics')
    op.create_index('ix_metrics_bibcode', 'metrics', ['bibcode'], unique=True)
    ### end Alembic commands ###