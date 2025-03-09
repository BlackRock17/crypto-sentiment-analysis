"""
Tests for token categorization API endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from src.main import app
from src.data_processing.models.database import (
    BlockchainToken, BlockchainNetwork, TokenCategorizationHistory
)
from src.data_processing.crud.create import (
    create_blockchain_token, create_blockchain_network
)
from src.data_processing.crud.update import update_token_blockchain_network
from src.data_processing.crud.auth import create_user


@pytest.fixture
def auth_headers():
    """Create an admin user and get auth headers."""
    from src.security.utils import create_access_token

    # Create a token for admin user
    token_data = {"sub": "admin", "user_id": 1}
    access_token = create_access_token(token_data)

    return {"Authorization": f"Bearer {access_token}"}


@pytest.fixture
def test_token_and_networks(db: Session):
    """Create test networks and token."""
    # Create networks
    network1 = create_blockchain_network(
        db=db,
        name="apitest1",
        display_name="API Test Network 1"
    )

    network2 = create_blockchain_network(
        db=db,
        name="apitest2",
        display_name="API Test Network 2"
    )

    # Create token
    token = create_blockchain_token(
        db=db,
        token_address="0xapitest",
        symbol="APITEST",
        name="API Test Token",
        blockchain_network=None
    )

    # Return created objects
    yield token, network1, network2

    # Clean up
    # Find and delete any history records
    history_records = db.query(TokenCategorizationHistory).filter(
        TokenCategorizationHistory.token_id == token.id
    ).all()

    for record in history_records:
        db.delete(record)

    db.delete(token)
    db.delete(network1)
    db.delete(network2)
    db.commit()


def test_token_categorization_endpoint(db: Session, auth_headers, test_token_and_networks):
    """Test token categorization endpoint."""
    token, network1, network2 = test_token_and_networks

    client = TestClient(app)

    # Create admin user for auth
    try:
        admin = create_user(
            db=db,
            username="admin",
            email="admin@example.com",
            password="securepassword",
            is_superuser=True
        )

        # Set token network
        response = client.post(
            f"/twitter/tokens/{token.id}/set-network",
            params={"network_id": network1.id, "confidence": 0.8},
            headers=auth_headers
        )

        assert response.status_code == 200
        assert response.json()["blockchain_network"] == network1.name

        # Get token history
        response = client.get(
            f"/twitter/tokens/{token.id}/categorization-history",
            headers=auth_headers
        )

        assert response.status_code == 200
        assert len(response.json()) == 1
        assert response.json()[0]["new_network_name"] == network1.name

    finally:
        # Clean up admin user
        if 'admin' in locals():
            db.delete(admin)
            db.commit()


def test_token_analyze_endpoint(db: Session, auth_headers, test_token_and_networks):
    """Test token analysis endpoint."""
    token, network1, network2 = test_token_and_networks

    # Create a test tweet mentioning a network
    from src.data_processing.crud.create import create_tweet, create_token_mention
    from datetime import datetime

    tweet = create_tweet(
        db=db,
        tweet_id="apitesttweet",
        text=f"Check out this new token on {network1.name}! $APITEST is great.",
        created_at=datetime.utcnow(),
        author_id="testuser",
        author_username="testuser"
    )

    mention = create_token_mention(
        db=db,
        tweet_id=tweet.id,
        token_id=token.id
    )

    client = TestClient(app)

    try:
        # Create admin user for auth
        admin = create_user(
            db=db,
            username="admin",
            email="admin@example.com",
            password="securepassword",
            is_superuser=True
        )

        # Call analysis endpoint
        response = client.get(
            f"/twitter/tokens/{token.id}/analyze",
            headers=auth_headers
        )

        assert response.status_code == 200
        assert response.json()["token_id"] == token.id
        assert response.json()["token_symbol"] == token.symbol
        assert "detected_networks" in response.json()

    finally:
        # Clean up
        db.delete(mention)
        db.delete(tweet)
        if 'admin' in locals():
            db.delete(admin)
        db.commit()
