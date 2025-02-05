from sqlalchemy.orm import Session
from src.data_processing.models.database import SolanaToken
from datetime import datetime


def create_solana_token(db: Session, token_address: str, symbol: str, name: str = None) -> SolanaToken:
    """
    Create a new Solana token record

    Args:
        db: Database session
        token_address: The token's address on Solana blockchain
        symbol: Token symbol (e.g. 'SOL')
        name: Optional full name of the token

    Returns:
        Created SolanaToken instance
    """
    db_token = SolanaToken(
        token_address=token_address,
        symbol=symbol,
        name=name,
        created_at=datetime.utcnow()
    )

    db.add(db_token)
    db.commit()
    db.refresh(db_token)

    return db_token