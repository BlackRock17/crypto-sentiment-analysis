"""
Tests for Twitter API endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime

from src.main import app
from src.data_processing.database import get_db
from src.data_processing.crud.auth import create_user
from src.data_processing.crud.twitter import create_influencer, get_influencer_by_username
from src.data_processing.models.database import BlockchainNetwork, BlockchainToken  # Updated imports

client = TestClient(app)


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def auth_headers(db: Session):
    """Create a user and get auth headers for tests"""
    timestamp = datetime.utcnow().timestamp()
    username = f"twitter_test_user_{timestamp}"
    password = "testpassword"

    # Create a test user (admin)
    user = create_user(
        db=db,
        username=username,
        email=f"twitter_test_{timestamp}@example.com",
        password=password,
        is_superuser=True
    )

    # Login to get token
    login_response = client.post(
        "/auth/token",
        data={"username": username, "password": password}
    )

    token_data = login_response.json()
    headers = {"Authorization": f"Bearer {token_data['access_token']}"}

    yield headers, user

    # Clean up
    db.delete(user)
    db.commit()


@pytest.fixture
def test_influencer(db: Session):
    """Create a test influencer"""
    timestamp = datetime.utcnow().timestamp()
    username = f"api_test_influencer_{timestamp}"

    influencer = create_influencer(
        db=db,
        username=username,
        name="API Test Influencer",
        description="Test description for API",
        is_active=True,
        is_automated=False
    )

    yield influencer

    # Clean up
    db.delete(influencer)
    db.commit()


@pytest.fixture
def test_blockchain_network(db: Session):
    """Create a test blockchain network"""
    network = BlockchainNetwork(
        name="testnet",
        display_name="Test Network",
        description="Test blockchain network for API tests",
        hashtags=["test", "testnet"],
        keywords=["test", "blockchain"]
    )

    db.add(network)
    db.commit()
    db.refresh(network)

    yield network

    # Clean up
    db.delete(network)
    db.commit()


@pytest.fixture
def test_blockchain_token(db: Session, test_blockchain_network):
    """Create a test blockchain token"""
    token = BlockchainToken(
        token_address="apitest123456789",
        symbol="APITEST",
        name="API Test Token",
        blockchain_network=test_blockchain_network.name,
        blockchain_network_id=test_blockchain_network.id,
        network_confidence=0.9,
        manually_verified=True
    )

    db.add(token)
    db.commit()
    db.refresh(token)

    yield token

    # Clean up
    db.delete(token)
    db.commit()


def test_get_twitter_status(auth_headers):
    """Test getting Twitter status"""
    headers, _ = auth_headers

    response = client.get("/twitter/status", headers=headers)

    assert response.status_code == 200
    data = response.json()

    assert "twitter_connection" in data
    assert "stored_tweets" in data
    assert "token_mentions" in data
    assert "collection_frequency" in data
    assert "influencers" in data

    print("✓ Successfully tested getting Twitter status")


def test_get_influencers_list(auth_headers, test_influencer, db: Session):
    """Test getting list of influencers"""
    headers, _ = auth_headers

    response = client.get("/twitter/influencers", headers=headers)

    assert response.status_code == 200
    data = response.json()

    assert isinstance(data, list)
    assert len(data) > 0

    # Find our test influencer in the list
    found = False
    for influencer in data:
        if influencer["id"] == test_influencer.id:
            found = True
            assert influencer["username"] == test_influencer.username
            break

    assert found, "Test influencer not found in the list"

    print("✓ Successfully tested getting influencers list")


def test_create_influencer_endpoint(auth_headers, db: Session):
    """Test creating a new influencer"""
    headers, _ = auth_headers

    timestamp = datetime.utcnow().timestamp()
    username = f"created_influencer_{timestamp}"

    influencer_data = {
        "username": username,
        "name": "Created Influencer",
        "description": "Test description for created influencer",
        "is_active": True,
        "is_automated": True,
        "priority": 50
    }

    response = client.post(
        "/twitter/influencers",
        json=influencer_data,
        headers=headers
    )

    assert response.status_code == 201
    data = response.json()

    assert data["username"] == username
    assert data["name"] == "Created Influencer"
    assert data["is_automated"] == True
    assert data["priority"] == 50

    # Verify in database
    created = get_influencer_by_username(db, username)
    assert created is not None

    # Clean up
    db.delete(created)
    db.commit()

    print("✓ Successfully tested creating an influencer")


def test_get_influencer_endpoint(auth_headers, test_influencer):
    """Test getting an influencer by ID"""
    headers, _ = auth_headers

    response = client.get(
        f"/twitter/influencers/{test_influencer.id}",
        headers=headers
    )

    assert response.status_code == 200
    data = response.json()

    assert data["id"] == test_influencer.id
    assert data["username"] == test_influencer.username

    print("✓ Successfully tested getting an influencer by ID")


def test_update_influencer_endpoint(auth_headers, test_influencer, db: Session):
    """Test updating an influencer"""
    headers, _ = auth_headers

    update_data = {
        "name": "Updated API Influencer",
        "description": "Updated description",
        "priority": 75
    }

    response = client.put(
        f"/twitter/influencers/{test_influencer.id}",
        json=update_data,
        headers=headers
    )

    assert response.status_code == 200
    data = response.json()

    assert data["id"] == test_influencer.id
    assert data["name"] == "Updated API Influencer"
    assert data["description"] == "Updated description"
    assert data["priority"] == 75

    # Verify in database
    db.refresh(test_influencer)
    assert test_influencer.name == "Updated API Influencer"

    print("✓ Successfully tested updating an influencer")


def test_toggle_influencer_automation_endpoint(auth_headers, test_influencer, db: Session):
    """Test toggling influencer automation"""
    headers, _ = auth_headers

    # Get initial state
    initial_state = test_influencer.is_automated

    response = client.post(
        f"/twitter/influencers/{test_influencer.id}/toggle-automation",
        headers=headers
    )

    assert response.status_code == 200
    data = response.json()

    assert data["id"] == test_influencer.id
    assert data["is_automated"] == (not initial_state)

    # Verify in database
    db.refresh(test_influencer)
    assert test_influencer.is_automated == (not initial_state)

    print("✓ Successfully tested toggling influencer automation")


def test_add_manual_tweet(auth_headers, test_influencer, db: Session):
    """Test manually adding a tweet"""
    headers, _ = auth_headers

    # Генериране на уникален tweet_id
    unique_tweet_id = f"manual_{int(datetime.utcnow().timestamp())}"

    # Осигуряване че всички задължителни полета са включени
    tweet_data = {
        "influencer_username": test_influencer.username,
        "text": "This is a manual test tweet about $SOL and $RAY #solana",
        "created_at": datetime.utcnow().isoformat(),  # Предостави времева марка в ISO формат
        "tweet_id": unique_tweet_id,  # Уникален tweet_id
        "retweet_count": 5,
        "like_count": 10
    }

    response = client.post(
        "/twitter/manual-tweets",
        json=tweet_data,
        headers=headers
    )

    assert response.status_code == 200
    data = response.json()

    assert "id" in data
    assert data["text"] == tweet_data["text"]
    assert data["author_username"] == test_influencer.username
    assert data["retweet_count"] == 5
    assert data["like_count"] == 10

    print("✓ Successfully tested adding a manual tweet")


def test_get_twitter_settings(auth_headers):
    """Test getting Twitter settings"""
    headers, _ = auth_headers

    response = client.get("/twitter/settings", headers=headers)

    assert response.status_code == 200
    data = response.json()

    assert "max_automated_influencers" in data
    assert "collection_frequency" in data
    assert "max_tweets_per_user" in data
    assert "daily_request_limit" in data
    assert "monthly_request_limit" in data

    print("✓ Successfully tested getting Twitter settings")


def test_get_api_usage(auth_headers, test_influencer, db: Session):
    """Test getting API usage statistics"""
    headers, _ = auth_headers

    response = client.get("/twitter/api-usage", headers=headers)

    assert response.status_code == 200
    data = response.json()

    assert "date" in data
    assert "daily_usage" in data
    assert "monthly_usage" in data
    assert "daily_limit" in data
    assert "monthly_limit" in data
    assert "remaining_daily" in data
    assert "remaining_monthly" in data

    print("✓ Successfully tested getting API usage statistics")


def test_delete_influencer_endpoint(auth_headers, db: Session):
    """Test deleting an influencer"""
    headers, _ = auth_headers

    # Create an influencer to delete
    timestamp = datetime.utcnow().timestamp()
    username = f"delete_test_api_{timestamp}"

    influencer = create_influencer(
        db=db,
        username=username,
        name="Delete Test API"
    )

    influencer_id = influencer.id

    # Delete the influencer
    response = client.delete(
        f"/twitter/influencers/{influencer_id}",
        headers=headers
    )

    assert response.status_code == 204

    # Verify it's gone
    deleted = get_influencer_by_username(db, username)
    assert deleted is None

    print("✓ Successfully tested deleting an influencer")


def test_blockchain_networks_endpoint(auth_headers, test_blockchain_network):
    """Test getting blockchain networks"""
    headers, _ = auth_headers

    response = client.get("/twitter/networks", headers=headers)

    assert response.status_code == 200
    data = response.json()

    assert isinstance(data, list)

    # Find our test network
    found = False
    for network in data:
        if network["id"] == test_blockchain_network.id:
            found = True
            assert network["name"] == test_blockchain_network.name
            assert network["display_name"] == test_blockchain_network.display_name
            break

    assert found, "Test blockchain network not found in the list"

    print("✓ Successfully tested getting blockchain networks")


def test_blockchain_tokens_endpoint(auth_headers, test_blockchain_token):
    """Test getting blockchain tokens"""
    headers, _ = auth_headers

    response = client.get("/twitter/tokens", headers=headers)

    assert response.status_code == 200
    data = response.json()

    assert isinstance(data, list)

    # Find our test token
    found = False
    for token in data:
        if token["id"] == test_blockchain_token.id:
            found = True
            assert token["symbol"] == test_blockchain_token.symbol
            assert token["blockchain_network"] == test_blockchain_token.blockchain_network
            break

    assert found, "Test blockchain token not found in the list"

    print("✓ Successfully tested getting blockchain tokens")


def test_create_blockchain_network_endpoint(auth_headers, db: Session):
    """Test creating a new blockchain network"""
    headers, _ = auth_headers

    timestamp = datetime.utcnow().timestamp()
    network_name = f"testnet_{timestamp}"

    network_data = {
        "name": network_name,
        "display_name": "Test Network API",
        "description": "Test network created via API",
        "hashtags": ["test", "api"],
        "keywords": ["test", "network", "api"],
        "is_active": True
    }

    response = client.post(
        "/twitter/networks",
        json=network_data,
        headers=headers
    )

    assert response.status_code == 201
    data = response.json()

    assert data["name"] == network_name
    assert data["display_name"] == "Test Network API"

    # Clean up - find and delete the created network
    created_network = db.query(BlockchainNetwork).filter(BlockchainNetwork.name == network_name).first()
    if created_network:
        db.delete(created_network)
        db.commit()

    print("✓ Successfully tested creating a blockchain network")


def test_create_blockchain_token_endpoint(auth_headers, test_blockchain_network, db: Session):
    """Test creating a new blockchain token"""
    headers, _ = auth_headers

    timestamp = datetime.utcnow().timestamp()
    token_symbol = f"TST{int(timestamp) % 10000}"

    token_data = {
        "token_address": f"address_{timestamp}",
        "symbol": token_symbol,
        "name": "Test Token API",
        "blockchain_network": test_blockchain_network.name
    }

    response = client.post(
        "/twitter/tokens",
        json=token_data,
        headers=headers
    )

    assert response.status_code == 201
    data = response.json()

    assert data["symbol"] == token_symbol
    assert data["blockchain_network"] == test_blockchain_network.name

    # Clean up - find and delete the created token
    created_token = db.query(BlockchainToken).filter(BlockchainToken.symbol == token_symbol).first()
    if created_token:
        db.delete(created_token)
        db.commit()

    print("✓ Successfully tested creating a blockchain token")
