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
        Генерира симулирани туитове и ги добавя чрез стандартния път за обработка.
        """
        try:
            # Симулирани потребители и техни данни
            influencers = [
                {"username": "crypto_expert"},
                {"username": "blockchain_news"},
                {"username": "coin_analyst"}
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

            # Функция за добавяне на туит чрез стандартния път
            from src.data_collection.tasks.twitter_tasks import _async_add_manual_tweet
            import asyncio

            # Създаваме нов event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Генериране и добавяне на мок туитове
            processed_tweets = 0
            total_tweets = random.randint(5, 15)  # Генерираме между 5 и 15 туита

            for i in range(total_tweets):
                influencer = random.choice(influencers)
                template = random.choice(tweet_templates)

                # Заместване на токени и хештагове
                token1 = random.choice(tokens)
                token2 = random.choice([t for t in tokens if t != token1])
                hashtag = random.choice(hashtags)

                text = template.replace("TOKEN1", token1).replace("TOKEN2", token2).replace("HASHTAG", hashtag)

                # Генериране на tweet_id
                tweet_id = f"mock_{random.randint(10000, 99999)}"

                # Добавяне на туита чрез стандартния път
                success = loop.run_until_complete(_async_add_manual_tweet(
                    influencer_username=influencer["username"],
                    tweet_text=text,
                    created_at=datetime.utcnow() - timedelta(hours=random.randint(1, 24)),
                    tweet_id=tweet_id,
                    retweet_count=random.randint(0, 100),
                    like_count=random.randint(10, 500)
                ))

                if success:
                    processed_tweets += 1
                    logger.info(f"Успешно добавен симулиран туит {i + 1}/{total_tweets}: '{text[:50]}...'")
                else:
                    logger.warning(f"Неуспешно добавяне на симулиран туит {i + 1}/{total_tweets}")

            # Затваряме loop-а
            loop.close()

            return {
                "generated_tweets": total_tweets,
                "processed_tweets": processed_tweets
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
            logger.info(f"Успешно генериране: {result.get('generated_tweets', 0)} туита")
            logger.info(f"Изпратени към Kafka: {result.get('processed_tweets', 0)} туита")

            return f"Генерирани {result.get('generated_tweets', 0)} симулирани туита, обработени {result.get('processed_tweets', 0)}"
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
