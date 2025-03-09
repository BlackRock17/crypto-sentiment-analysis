"""
CRUD operations for token categorization.
"""
from typing import Optional, List, Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session

from src.data_processing.models.database import (
    BlockchainToken, BlockchainNetwork, TokenCategorizationHistory
)


def create_categorization_record(
        db: Session,
        token_id: int,
        new_network_id: int,
        new_confidence: float,
        user_id: Optional[int] = None,
        is_auto_categorized: bool = False,
        notes: Optional[str] = None
) -> TokenCategorizationHistory:
    """
    Create a record of token categorization.

    Args:
        db: Database session
        token_id: ID of the token being categorized
        new_network_id: ID of the new network assigned to the token
        new_confidence: New confidence score for the categorization
        user_id: ID of the user performing the categorization (None for automated)
        is_auto_categorized: Whether the categorization was automated
        notes: Optional notes about the categorization

    Returns:
        Created TokenCategorizationHistory instance
    """
    # Get the token
    token = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()
    if not token:
        raise ValueError(f"Token with ID {token_id} not found")

    # Get the previous network ID if any
    previous_network_id = token.blockchain_network_id
    previous_confidence = token.network_confidence

    # Create the history record
    history = TokenCategorizationHistory(
        token_id=token_id,
        previous_network_id=previous_network_id,
        new_network_id=new_network_id,
        previous_confidence=previous_confidence,
        new_confidence=new_confidence,
        categorized_by_user_id=user_id,
        is_auto_categorized=is_auto_categorized,
        notes=notes,
        created_at=datetime.utcnow()
    )

    db.add(history)
    db.commit()
    db.refresh(history)

    return history


def get_token_categorization_history(
        db: Session,
        token_id: int,
        limit: int = 10
) -> List[TokenCategorizationHistory]:
    """
    Get the categorization history for a specific token.

    Args:
        db: Database session
        token_id: ID of the token
        limit: Maximum number of records to return

    Returns:
        List of TokenCategorizationHistory instances
    """
    return db.query(TokenCategorizationHistory).filter(
        TokenCategorizationHistory.token_id == token_id
    ).order_by(
        TokenCategorizationHistory.created_at.desc()
    ).limit(limit).all()


def get_recent_categorizations(
        db: Session,
        limit: int = 20,
        user_id: Optional[int] = None,
        auto_only: bool = False
) -> List[TokenCategorizationHistory]:
    """
    Get recent token categorizations.

    Args:
        db: Database session
        limit: Maximum number of records to return
        user_id: Filter by user ID
        auto_only: Only return automated categorizations

    Returns:
        List of TokenCategorizationHistory instances
    """
    query = db.query(TokenCategorizationHistory)

    if user_id is not None:
        query = query.filter(TokenCategorizationHistory.categorized_by_user_id == user_id)

    if auto_only:
        query = query.filter(TokenCategorizationHistory.is_auto_categorized == True)

    return query.order_by(TokenCategorizationHistory.created_at.desc()).limit(limit).all()


def get_categorization_stats(
        db: Session,
        days_back: int = 30
) -> Dict[str, Any]:
    """
    Get statistics about token categorizations.

    Args:
        db: Database session
        days_back: Number of days to look back

    Returns:
        Dictionary with categorization statistics
    """
    from datetime import datetime, timedelta
    from sqlalchemy import func, distinct

    # Calculate the cutoff date
    cutoff_date = datetime.utcnow() - timedelta(days=days_back)

    # Base query for recent categorizations
    base_query = db.query(TokenCategorizationHistory).filter(
        TokenCategorizationHistory.created_at >= cutoff_date
    )

    # Total categorizations
    total_count = base_query.count()

    # Count by type (auto vs manual)
    auto_count = base_query.filter(TokenCategorizationHistory.is_auto_categorized == True).count()
    manual_count = total_count - auto_count

    # Count by network
    network_counts = db.query(
        BlockchainNetwork.name,
        func.count(TokenCategorizationHistory.id)
    ).join(
        TokenCategorizationHistory,
        TokenCategorizationHistory.new_network_id == BlockchainNetwork.id
    ).filter(
        TokenCategorizationHistory.created_at >= cutoff_date
    ).group_by(
        BlockchainNetwork.name
    ).all()

    # Count unique tokens categorized
    unique_tokens = base_query.with_entities(
        func.count(distinct(TokenCategorizationHistory.token_id))
    ).scalar()

    # Count recategorizations (tokens categorized more than once)
    recategorization_counts = db.query(
        TokenCategorizationHistory.token_id,
        func.count(TokenCategorizationHistory.id).label("count")
    ).filter(
        TokenCategorizationHistory.created_at >= cutoff_date
    ).group_by(
        TokenCategorizationHistory.token_id
    ).having(
        func.count(TokenCategorizationHistory.id) > 1
    ).count()

    # Format network counts
    networks = {name: count for name, count in network_counts}

    return {
        "period": f"{cutoff_date.strftime('%Y-%m-%d')} to {datetime.utcnow().strftime('%Y-%m-%d')}",
        "total_categorizations": total_count,
        "auto_categorizations": auto_count,
        "manual_categorizations": manual_count,
        "unique_tokens_categorized": unique_tokens,
        "tokens_recategorized": recategorization_counts,
        "categorizations_by_network": networks
    }
