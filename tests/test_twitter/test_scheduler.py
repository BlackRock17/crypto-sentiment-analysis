"""
Tests for the scheduler functionality.
"""

import pytest
import src.scheduler.scheduler
from unittest.mock import patch, MagicMock

from src.scheduler.scheduler import setup_scheduler, shutdown_scheduler


@pytest.fixture
def mock_apscheduler():
    """Mock the APScheduler components"""
    with patch('src.scheduler.scheduler.AsyncIOScheduler') as mock_scheduler, \
            patch('src.scheduler.scheduler.SQLAlchemyJobStore') as mock_jobstore, \
            patch('src.scheduler.scheduler.ThreadPoolExecutor') as mock_executor, \
            patch('src.scheduler.scheduler._configure_scheduled_jobs') as mock_configure:
        # Set up mock scheduler behavior
        mock_instance = MagicMock()
        mock_scheduler.return_value = mock_instance

        yield {
            'scheduler_class': mock_scheduler,
            'scheduler_instance': mock_instance,
            'jobstore': mock_jobstore,
            'executor': mock_executor,
            'configure_jobs': mock_configure
        }


def test_setup_scheduler(mock_apscheduler):
    """Test scheduler setup"""
    # Run the function
    result = setup_scheduler()

    # Verify the scheduler was created with correct parameters
    mock_apscheduler['scheduler_class'].assert_called_once()
    mock_apscheduler['jobstore'].assert_called_once()
    mock_apscheduler['executor'].assert_called_once()

    # Verify jobs were configured
    mock_apscheduler['configure_jobs'].assert_called_once()

    # Verify scheduler was started
    mock_apscheduler['scheduler_instance'].start.assert_called_once()

    # Verify the function returns the scheduler instance
    assert result == mock_apscheduler['scheduler_instance']


def test_shutdown_scheduler(mock_apscheduler):
    """Test scheduler shutdown"""
    # Temporarily replace the real scheduler with a mock
    original_scheduler = src.scheduler.scheduler.scheduler
    src.scheduler.scheduler.scheduler = mock_apscheduler['scheduler_instance']

    try:
        # First set up the scheduler
        setup_scheduler()

        # Then shut it down
        shutdown_scheduler()

        # Verify the scheduler was shut down
        mock_apscheduler['scheduler_instance'].shutdown.assert_called_once()
    finally:
        # Restore original
        src.scheduler.scheduler.scheduler = original_scheduler


def test_setup_scheduler_twice():
    """Test that setup_scheduler doesn't recreate the scheduler if called multiple times"""
    # Using direct patching in the test instead of the fixture
    with patch('src.scheduler.scheduler.AsyncIOScheduler') as mock_scheduler, \
         patch('src.scheduler.scheduler.SQLAlchemyJobStore'), \
         patch('src.scheduler.scheduler.ThreadPoolExecutor'), \
         patch('src.scheduler.scheduler._configure_scheduled_jobs'):
        # Mock instance
        mock_instance = MagicMock()
        mock_scheduler.return_value = mock_instance

        # Set scheduler to None to ensure that a new one will be created.
        import src.scheduler.scheduler
        src.scheduler.scheduler.scheduler = None

        # Calling setup_scheduler twice
        setup_scheduler()
        setup_scheduler()

        # Checking that the class has been called only once
        assert mock_scheduler.call_count == 1
        assert mock_instance.start.call_count == 1
