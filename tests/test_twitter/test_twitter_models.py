"""
Tests for Twitter models.
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_processing.models.twitter import TwitterInfluencer, TwitterInfluencerTweet, TwitterApiUsage
from src.data_processing.crud.twitter import (
    create_influencer, get_influencer, get_influencer_by_username,
    update_influencer, delete_influencer, toggle_influencer_automation,
    create_api_usage, get_api_usage_stats, get_api_usage_history
)


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def test_influencer(db: Session):
    """Create a test influencer"""
    timestamp = datetime.utcnow().timestamp()
    username = f"test_influencer_{timestamp}"

    influencer = create_influencer(
        db=db,
        username=username,
        name="Test Influencer",
        description="Test description",
        follower_count=1000,
        is_active=True,
        is_automated=False,
        priority=5
    )

    yield influencer

    # Clean up
    db.delete(influencer)
    db.commit()


def test_create_influencer(db: Session):
    """Test creating a Twitter influencer"""
    timestamp = datetime.utcnow().timestamp()
    username = f"create_test_{timestamp}"

    influencer = create_influencer(
        db=db,
        username=username,
        name="Create Test",
        description="Test description",
        follower_count=1000,
        is_active=True,
        is_automated=True,
        priority=10
    )

    assert influencer is not None
    assert influencer.username == username
    assert influencer.name == "Create Test"
    assert influencer.follower_count == 1000
    assert influencer.is_active == True
    assert influencer.is_automated == True
    assert influencer.priority == 10

    # Clean up
    db.delete(influencer)
    db.commit()

    print("✓ Successfully tested creating an influencer")


def test_get_influencer(db: Session, test_influencer):
    """Test getting an influencer by ID"""
    influencer = get_influencer(db, test_influencer.id)

    assert influencer is not None
    assert influencer.id == test_influencer.id
    assert influencer.username == test_influencer.username

    print("✓ Successfully tested getting an influencer by ID")


def test_get_influencer_by_username(db: Session, test_influencer):
    """Test getting an influencer by username"""
    influencer = get_influencer_by_username(db, test_influencer.username)

    assert influencer is not None
    assert influencer.id == test_influencer.id
    assert influencer.username == test_influencer.username

    print("✓ Successfully tested getting an influencer by username")


def test_update_influencer(db: Session, test_influencer):
    """Test updating an influencer"""
    # Update influencer
    updated = update_influencer(
        db=db,
        influencer_id=test_influencer.id,
        name="Updated Name",
        description="Updated description",
        follower_count=2000,
        priority=20
    )

    assert updated is not None
    assert updated.id == test_influencer.id
    assert updated.name == "Updated Name"
    assert updated.description == "Updated description"
    assert updated.follower_count == 2000
    assert updated.priority == 20

    print("✓ Successfully tested updating an influencer")


def test_toggle_influencer_automation(db: Session, test_influencer):
    """Test toggling influencer automation"""
    # Get current state
    initial_state = test_influencer.is_automated

    # Toggle automation
    updated = toggle_influencer_automation(db, test_influencer.id)

    assert updated is not None
    assert updated.id == test_influencer.id
    assert updated.is_automated == (not initial_state)

    # Toggle back
    updated = toggle_influencer_automation(db, test_influencer.id)

    assert updated.is_automated == initial_state

    print("✓ Successfully tested toggling influencer automation")


def test_delete_influencer(db: Session):
    """Test deleting an influencer"""
    # Create an influencer to delete
    timestamp = datetime.utcnow().timestamp()
    username = f"delete_test_{timestamp}"

    influencer = create_influencer(
        db=db,
        username=username,
        name="Delete Test"
    )

    influencer_id = influencer.id

    # Delete the influencer
    result = delete_influencer(db, influencer_id)

    assert result is True

    # Verify it's gone
    deleted = get_influencer(db, influencer_id)
    assert deleted is None

    print("✓ Successfully tested deleting an influencer")


def test_api_usage(db: Session, test_influencer):
    """Test API usage tracking"""
    # Create API usage record
    now = datetime.utcnow()
    usage = create_api_usage(
        db=db,
        influencer_id=test_influencer.id,
        endpoint="user_tweets",
        requests_used=1,
        date=now
    )

    assert usage is not None
    assert usage.influencer_id == test_influencer.id
    assert usage.endpoint == "user_tweets"
    assert usage.requests_used == 1

    # Get usage stats
    stats = get_api_usage_stats(db, now.date())

    assert stats is not None
    assert stats["daily_usage"] >= 1
    assert len(stats["influencer_usage"]) > 0

    # Clean up
    db.delete(usage)
    db.commit()

    print("✓ Successfully tested API usage tracking")


def test_get_api_usage_history(db: Session, test_influencer):
    """Test getting API usage history"""
    # Create some usage records over multiple days
    base_date = datetime.utcnow() - timedelta(days=5)

    usages = []
    for i in range(5):
        date = base_date + timedelta(days=i)
        usage = create_api_usage(
            db=db,
            influencer_id=test_influencer.id,
            endpoint="user_tweets",
            requests_used=i + 1,
            date=date
        )
        usages.append(usage)

    # Get usage history
    history = get_api_usage_history(db, days=10)

    assert history is not None
    assert len(history) >= 5  # May be more if other tests have run

    # Clean up
    for usage in usages:
        db.delete(usage)
    db.commit()

    print("✓ Successfully tested getting API usage history")
