"""
Pytest configuration file for the application tests.
This file is automatically detected by pytest and contains global fixtures.
"""
import os
import pytest
from sqlalchemy.orm import Session

from src.data_processing.database import get_db

@pytest.fixture(scope="session", autouse=True)
def set_test_environment():
    """
    Set the TESTING environment variable to true for all tests.
    This fixture runs automatically before any tests.
    """
    os.environ["TESTING"] = "true"
    yield
    # Clean up after all tests
    os.environ.pop("TESTING", None)

@pytest.fixture
def db():
    """
    Provide a database session fixture
    """
    session = next(get_db())
    try:
        yield session
    finally:
        session.close()
