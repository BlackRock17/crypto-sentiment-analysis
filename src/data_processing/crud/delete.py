from sqlalchemy.orm import Session
from src.data_processing.models.database import SolanaToken, Tweet, SentimentAnalysis, TokenMention
from datetime import datetime


def delete_solana_token(db: Session, token_id: int, check_mentions: bool = True) -> bool:
    """
    Delete a Solana token record

    Args:
        db: Database session
        token_id: The ID of the token to delete
        check_mentions: If True, will check if token has mentions and prevent deletion

    Returns:
        True if deletion was successful, False if token not found or has mentions
    """
    # Get the token by ID
    db_token = db.query(SolanaToken).filter(SolanaToken.id == token_id).first()

    # Return False if token doesn't exist
    if db_token is None:
        return False

    # Check if the token has mentions
    if check_mentions:
        mentions_count = db.query(TokenMention).filter(TokenMention.token_id == token_id).count()
        if mentions_count > 0:
            raise ValueError(
                f"Cannot delete token with ID {token_id} as it has {mentions_count} mentions. Remove mentions first or use cascade delete.")

    # Delete the token
    db.delete(db_token)
    db.commit()

    return True