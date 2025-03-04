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

    tweet_data = {
        "influencer_username": test_influencer.username,
        "text": "This is a manual test tweet about $SOL and $RAY #solana",
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
