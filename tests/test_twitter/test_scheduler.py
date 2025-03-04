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