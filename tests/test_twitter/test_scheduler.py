"""
Tests for the scheduler functionality.
"""

import pytest
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
    # First set up the scheduler
    setup_scheduler()

    # Then shut it down
    shutdown_scheduler()

    # Verify the scheduler was shut down
    mock_apscheduler['scheduler_instance'].shutdown.assert_called_once()


def test_setup_scheduler_twice(mock_apscheduler):
    """Test that setup_scheduler doesn't recreate the scheduler if called multiple times"""
    # Call setup_scheduler twice
    setup_scheduler()
    setup_scheduler()

    # Verify scheduler was created only once
    assert mock_apscheduler['scheduler_class'].call_count == 1
    assert mock_apscheduler['scheduler_instance'].start.call_count == 1
