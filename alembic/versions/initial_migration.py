"""initial migration

Revision ID: initial_migration
Create Date: 2024-02-02 12:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def upgrade():
    # Creating an enum type for sentiment
    op.create_table(
        'tweets',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tweet_id', sa.String(length=50), nullable=False),
        sa.Column('text', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('author_id', sa.String(length=50), nullable=False),
        sa.Column('author_username', sa.String(length=100)),
        sa.Column('retweet_count', sa.Integer(), default=0),
        sa.Column('like_count', sa.Integer(), default=0),
        sa.Column('collected_at', sa.DateTime(), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tweet_id')
    )

    # Create an index for faster search by tweet_id
    op.create_index('idx_tweets_tweet_id', 'tweets', ['tweet_id'])

    op.create_table(
        'sentiment_analysis',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tweet_id', sa.Integer(), nullable=False),
        sa.Column('sentiment', sa.Enum('POSITIVE', 'NEGATIVE', 'NEUTRAL', name='sentimentenum'), nullable=False),
        sa.Column('confidence_score', sa.Float(), nullable=False),
        sa.Column('analyzed_at', sa.DateTime(), server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['tweet_id'], ['tweets.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tweet_id')
    )

    op.create_table(
        'solana_tokens',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('token_address', sa.String(length=44), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('name', sa.String(length=100)),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('token_address')
    )

    op.create_table(
        'token_mentions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tweet_id', sa.Integer(), nullable=False),
        sa.Column('token_id', sa.Integer(), nullable=False),
        sa.Column('mentioned_at', sa.DateTime(), server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['token_id'], ['solana_tokens.id'], ),
        sa.ForeignKeyConstraint(['tweet_id'], ['tweets.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Creating indexes for faster searching
    op.create_index('idx_token_mentions_tweet_id', 'token_mentions', ['tweet_id'])
    op.create_index('idx_token_mentions_token_id', 'token_mentions', ['token_id'])


def downgrade():
    op.drop_table('token_mentions')
    op.drop_table('solana_tokens')
    op.drop_table('sentiment_analysis')
    op.drop_table('tweets')
    op.execute('DROP TYPE sentimentenum')