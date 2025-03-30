"""
DAG за симулация на автоматично събиране на данни от Twitter.
Използва мок данни вместо реални API заявки.
"""
import os
import sys
from datetime import datetime, timedelta
import logging
import json
import random
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Конфигуриране на логър
logger = logging.getLogger(__name__)

airflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /opt/airflow
sys.path.append(airflow_dir)

# Пътища за мок данни
MOCK_DATA_DIR = Path("/opt/airflow/src/mock_data")
MOCK_DATA_DIR.mkdir(exist_ok=True)

# Стандартни аргументи за DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Създаване на DAG
with DAG(
        'simulated_tweet_collection',
        default_args=default_args,
        description='Симулирано събиране на туитове без истински API заявки',
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['twitter', 'data_collection', 'simulation']
) as dag:
    # Функция за генериране на симулирани туитове
    def generate_mock_tweets(**kwargs):
        """
        Генерира симулирани туитове, които могат да се използват за тестване на системата.
        """
        try:
            # Симулирани потребители и техни данни
            influencers = [
                {"username": "crypto_expert", "id": 1},
                {"username": "blockchain_news", "id": 2},
                {"username": "coin_analyst", "id": 3}
            ]

            # Токени за споменаване
            tokens = ["BTC", "ETH", "SOL", "ADA", "DOT", "AVAX"]

            # Хештагове
            hashtags = ["crypto", "blockchain", "defi", "nft", "ethereum", "solana"]

            # Шаблони за туитове
            tweet_templates = [
                "Just bought some $TOKEN1 and $TOKEN2. Feeling bullish! #HASHTAG",
                "The future of $TOKEN1 looks promising. #HASHTAG #crypto",
                "$TOKEN1 vs $TOKEN2 - which one would you choose? #HASHTAG #investing",
                "New developments in $TOKEN1 ecosystem are impressive! #HASHTAG",
                "Market sentiment for $TOKEN1 is changing. Stay tuned! #HASHTAG"
            ]

            # Генериране на мок туитове
            mock_tweets = []
            for _ in range(random.randint(5, 15)):  # Генерираме между 5 и 15 туита
                influencer = random.choice(influencers)
                template = random.choice(tweet_templates)

                # Заместване на токени и хештагове
                token1 = random.choice(tokens)
                token2 = random.choice([t for t in tokens if t != token1])
                hashtag = random.choice(hashtags)

                text = template.replace("TOKEN1", token1).replace("TOKEN2", token2).replace("HASHTAG", hashtag)

                # Създаване на симулиран туит
                tweet = {
                    "tweet_id": f"mock_{random.randint(10000, 99999)}",
                    "text": text,
                    "created_at": (datetime.utcnow() - timedelta(hours=random.randint(1, 24))).isoformat(),
                    "author_id": str(influencer["id"]),
                    "author_username": influencer["username"],
                    "retweet_count": random.randint(0, 100),
                    "like_count": random.randint(10, 500),
                    "influencer_id": influencer["id"],
                    "cashtags": [token1, token2],
                    "hashtags": [hashtag]
                }

                mock_tweets.append(tweet)

            # Запазване на мок данните
            mock_filename = MOCK_DATA_DIR / f"mock_tweets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(mock_filename, 'w') as f:
                json.dump(mock_tweets, f, indent=2)

            # Използване на съществуващия код за обработка
            try:
                from src.data_processing.database import get_db
                from src.data_processing.kafka.producer import TwitterProducer

                # Инициализация на Kafka producer
                producer = TwitterProducer()

                # Изпращане на туитовете към Kafka
                sent_count = 0
                for tweet in mock_tweets:
                    success = producer.send_tweet(tweet)
                    if success:
                        sent_count += 1

                logger.info(f"Успешно изпратени {sent_count} от {len(mock_tweets)} симулирани туита към Kafka")

                return {
                    "generated_tweets": len(mock_tweets),
                    "sent_to_kafka": sent_count,
                    "mock_file": str(mock_filename)
                }

            except ImportError as e:
                logger.warning(f"Не може да се импортира модул: {e}")
                logger.info(f"Генерирани {len(mock_tweets)} мок туита, запазени в {mock_filename}")
                return {
                    "generated_tweets": len(mock_tweets),
                    "sent_to_kafka": 0,
                    "mock_file": str(mock_filename)
                }

        except Exception as e:
            logger.error(f"Грешка при генериране на мок туитове: {e}")
            return {"error": str(e)}


    # Задача за генериране на симулирани туитове
    generate_tweets_task = PythonOperator(
        task_id='generate_mock_tweets',
        python_callable=generate_mock_tweets,
    )


    # Функция за отчет след генерирането
    def report_generation_results(ti, **kwargs):
        """
        Създава отчет за генерираните симулирани туитове.
        """
        result = ti.xcom_pull(task_ids='generate_mock_tweets')

        if result and not "error" in result:
            logger.info(f"Успешно генериране: {result['generated_tweets']} туита")
            logger.info(f"Изпратени към Kafka: {result['sent_to_kafka']} туита")
            logger.info(f"Запазени в: {result['mock_file']}")

            return f"Генерирани {result['generated_tweets']} симулирани туита"
        else:
            error = result.get("error", "Неизвестна грешка")
            logger.error(f"Неуспешно генериране: {error}")
            return f"Грешка при генериране: {error}"


    # Задача за отчет
    report_task = PythonOperator(
        task_id='report_generation',
        python_callable=report_generation_results,
    )

    # Задаване на последователността
    generate_tweets_task >> report_task
