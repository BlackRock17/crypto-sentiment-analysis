from sqlalchemy.orm import Session
from src.data_processing.models.database import SolanaToken, Tweet, SentimentEnum, SentimentAnalysis, TokenMention
from datetime import datetime


def update_solana_token(
        db: Session,
        token_id: int,
        symbol: str = None,
        name: str = None
) -> SolanaToken:
    """
    Update a Solana token record

    Args:
        db: Database session
        token_id: The ID of the token to update
        symbol: New token symbol (optional)
        name: New token name (optional)

    Returns:
        Updated SolanaToken instance or None if not found
    """
    # Get the token by ID
    db_token = db.query(SolanaToken).filter(SolanaToken.id == token_id).first()

    # Return None if token doesn't exist
    if db_token is None:
        return None

    # Update fields if provided
    if symbol is not None:
        db_token.symbol = symbol

    if name is not None:
        db_token.name = name

    # Commit changes to the database
    db.commit()
    db.refresh(db_token)

    return db_token