import logging
from src.data_processing.kafka.setup import ensure_all_topics_exist

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Creating Kafka topics...")
    success = ensure_all_topics_exist()

    if success:
        logger.info("All topics created successfully")
        return 0
    else:
        logger.error("Failed to create topics")
        return 1


if __name__ == "__main__":
    exit(main())
