"""
Конфигурация за Kafka, специфична за Airflow.
"""
# Bootstrap сървъри
DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092"

# Топици
TOPICS = {
    "RAW_TWEETS": "twitter-raw-tweets",
    "TOKEN_MENTIONS": "token-mentions",
    "SENTIMENT_RESULTS": "sentiment-results",
    "TOKEN_CATEGORIZATION": "token-categorization-tasks",
    "SYSTEM_NOTIFICATIONS": "system-notifications"
}