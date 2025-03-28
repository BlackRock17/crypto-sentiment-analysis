"""add token categorization history

Revision ID: e97cf4953e36
Revises: 8eb2b097025c
Create Date: 2025-03-09 22:00:20.269008

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e97cf4953e36'
down_revision: Union[str, None] = '8eb2b097025c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('token_categorization_history',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('token_id', sa.Integer(), nullable=False),
    sa.Column('previous_network_id', sa.Integer(), nullable=True),
    sa.Column('new_network_id', sa.Integer(), nullable=True),
    sa.Column('previous_confidence', sa.Float(), nullable=True),
    sa.Column('new_confidence', sa.Float(), nullable=False),
    sa.Column('categorized_by_user_id', sa.Integer(), nullable=True),
    sa.Column('is_auto_categorized', sa.Boolean(), nullable=True),
    sa.Column('notes', sa.String(length=255), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['categorized_by_user_id'], ['users.id'], ),
    sa.ForeignKeyConstraint(['new_network_id'], ['blockchain_networks.id'], ),
    sa.ForeignKeyConstraint(['previous_network_id'], ['blockchain_networks.id'], ),
    sa.ForeignKeyConstraint(['token_id'], ['blockchain_tokens.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('token_categorization_history')
    # ### end Alembic commands ###
