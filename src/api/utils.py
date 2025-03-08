"""
Helper functions for API responses and model mapping.
"""
from typing import Dict, Any, Optional, List, Type, TypeVar

from sqlalchemy.orm import Session

from src.data_processing.models.database import BlockchainToken, BlockchainNetwork

# Type variable for generic functions
T = TypeVar('T')


def enhance_token_response(token: BlockchainToken, db: Session) -> Dict[str, Any]:
    """
    Enhance a token model with network information for API responses.

    Args:
        token: BlockchainToken instance
        db: Database session

    Returns:
        Dictionary with token data enhanced with network information
    """
    # Convert token to dictionary
    token_dict = {
        "id": token.id,
        "token_address": token.token_address,
        "symbol": token.symbol,
        "name": token.name,
        "blockchain_network": token.blockchain_network,
        "blockchain_network_id": token.blockchain_network_id,
        "network_confidence": token.network_confidence,
        "manually_verified": token.manually_verified,
        "needs_review": token.needs_review,
        "created_at": token.created_at,
        "updated_at": token.updated_at,
        "network_name": None,
        "network_display_name": None
    }

    # Add network information if available
    if token.blockchain_network_id:
        network = db.query(BlockchainNetwork).filter(
            BlockchainNetwork.id == token.blockchain_network_id
        ).first()

        if network:
            token_dict["network_name"] = network.name
            token_dict["network_display_name"] = network.display_name

    return token_dict
