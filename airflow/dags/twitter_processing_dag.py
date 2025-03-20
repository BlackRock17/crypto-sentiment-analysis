from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import json
import os
import logging

# Достъпваме src директорията
sys.path.append('/opt/airflow/src')

# Настройка на логването
logger = logging.getLogger(__name__)

# Зареждане на модули от проекта
try:
    from confluent_kafka import Producer, Consumer, KafkaError
    from src.data_processing.kafka.config import TOPICS, DEFAULT_BOOTSTRAP_SERVERS
    from src.data_processing.kafka.producer import TwitterProducer
except ImportError as e:
    logger.error(f"Грешка при импортиране на модули: {e}")

# Задаване на аргументи по подразбиране
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Функция за събиране на туитове
def collect_tweets(**kwargs):
    logger.info("Започва събиране на примерни туитове")

    # Създаваме TwitterProducer
    try:
        producer = TwitterProducer()

        # Примерни туитове (за тестване)
        sample_tweets = [
            {
                "tweet_id": f"airflow_test_{int(datetime.now().timestamp())}",
                "text": "Airflow е много полезен инструмент за автоматизация на работни процеси! #Airflow #DataEngineering",
                "author_username": "airflow_tester",
                "author_id": "airflow_test_user",
                "created_at": datetime.now().isoformat(),
                "retweet_count": 42,
                "like_count": 100
            },
            {
                "tweet_id": f"airflow_test_crypto_{int(datetime.now().timestamp())}",
                "text": "Много съм впечатлен от $SOL и $ETH тази седмица! #Solana #Ethereum #Crypto",
                "author_username": "crypto_enthusiast",
                "author_id": "crypto_test_user",
                "created_at": datetime.now().isoformat(),
                "retweet_count": 24,
                "like_count": 78
            }
        ]

        # Изпращаме туитовете към Kafka
        success_count = 0
        for tweet in sample_tweets:
            success = producer.send_tweet(tweet)
            if success:
                success_count += 1
                logger.info(f"Успешно изпратен туит: {tweet['tweet_id']}")
            else:
                logger.error(f"Грешка при изпращане на туит: {tweet['tweet_id']}")

        # Flush на изходящите съобщения
        producer.flush()

        logger.info(f"Събрани и изпратени {success_count} туита")
        return success_count

    except Exception as e:
        logger.error(f"Грешка при събиране на туитове: {e}")
        return 0


# Функция за прочитане и обработка на туитове от Kafka
def process_tweets(**kwargs):
    logger.info("Започва обработка на туитове от Kafka")

    try:
        # Конфигурация на консуматора
        config = {
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'airflow-twitter-processor',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        }

        # Създаване на консуматор
        consumer = Consumer(config)

        # Абониране за топика с туитове
        consumer.subscribe([TOPICS['RAW_TWEETS']])

        # Списък за съхранение на обработени туитове
        processed_tweets = []

        # Опит за получаване на съобщения (с таймаут)
        start_time = datetime.now()
        timeout = timedelta(seconds=20)  # Максимално време за четене

        while datetime.now() - start_time < timeout:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Достигнат е краят на партиция {msg.topic()}")
                else:
                    logger.error(f"Грешка при четене: {msg.error()}")
            else:
                # Декодиране и обработка на съобщението
                try:
                    tweet_data = json.loads(msg.value().decode('utf-8'))

                    # Тук може да добавите допълнителна обработка на туитовете
                    # Например: анализ на настроения, извличане на споменати токени и т.н.

                    # За целите на демонстрацията ще извлечем само основна информация
                    processed_tweet = {
                        'tweet_id': tweet_data.get('tweet_id'),
                        'text': tweet_data.get('text'),
                        'author': tweet_data.get('author_username'),
                        'processed_at': datetime.now().isoformat()
                    }

                    processed_tweets.append(processed_tweet)
                    logger.info(f"Обработен туит: {processed_tweet['tweet_id']}")

                except Exception as e:
                    logger.error(f"Грешка при обработка на съобщение: {e}")

        # Затваряне на консуматора
        consumer.close()

        # Запазване на резултатите за следващата задача
        kwargs['ti'].xcom_push(key='processed_tweets', value=processed_tweets)

        logger.info(f"Общо обработени {len(processed_tweets)} туита")
        return len(processed_tweets)

    except Exception as e:
        logger.error(f"Грешка при обработка на туитове: {e}")
        return 0


# Функция за анализ на обработените туитове
def analyze_tweets(**kwargs):
    ti = kwargs['ti']

    # Вземане на обработените туитове от предишната задача
    processed_tweets = ti.xcom_pull(task_ids='process_tweets', key='processed_tweets')

    if not processed_tweets:
        logger.warning("Няма обработени туитове за анализ")
        return "Няма данни за анализ"

    logger.info(f"Започва анализ на {len(processed_tweets)} туита")

    # Пример за прост анализ - търсене на криптовалутни символи
    crypto_symbols = ['BTC', 'ETH', 'SOL', 'ADA', 'DOT']
    symbol_mentions = {symbol: 0 for symbol in crypto_symbols}

    for tweet in processed_tweets:
        text = tweet.get('text', '').upper()
        for symbol in crypto_symbols:
            if f"${symbol}" in text or f"#{symbol}" in text or symbol in text:
                symbol_mentions[symbol] += 1

    # Анализ на резултатите
    analysis_results = {
        'total_tweets': len(processed_tweets),
        'symbol_mentions': symbol_mentions,
        'most_mentioned': max(symbol_mentions.items(), key=lambda x: x[1]) if symbol_mentions else None,
        'analyzed_at': datetime.now().isoformat()
    }

    # Запазване на резултатите
    kwargs['ti'].xcom_push(key='analysis_results', value=analysis_results)

    logger.info(f"Анализ завършен: {json.dumps(analysis_results)}")
    return "Анализът завършен успешно"


# Функция за отчет на резултатите
def generate_report(**kwargs):
    ti = kwargs['ti']

    # Вземане на анализа от предишната задача
    analysis_results = ti.xcom_pull(task_ids='analyze_tweets', key='analysis_results')

    if not analysis_results:
        return "Няма данни за отчет"

    # Генериране на отчет
    report = f"""
    === ОТЧЕТ ЗА TWITTER АНАЛИЗ ===
    Дата и час: {datetime.now().isoformat()}

    Общ брой анализирани туитове: {analysis_results.get('total_tweets', 0)}

    Споменавания на криптовалути:
    """

    for symbol, count in analysis_results.get('symbol_mentions', {}).items():
        report += f"  - {symbol}: {count} споменавания\n"

    most_mentioned = analysis_results.get('most_mentioned')
    if most_mentioned:
        report += f"\nНай-често споменавана криптовалута: {most_mentioned[0]} ({most_mentioned[1]} пъти)"

    logger.info(report)
    return "Отчетът генериран успешно"


# Създаване на DAG
with DAG(
        'twitter_processing_dag',
        default_args=default_args,
        description='Обработка на Twitter данни за криптовалути',
        schedule_interval=timedelta(hours=1),
        catchup=False
) as dag:
    # Задача 1: Събиране на туитове
    collect_task = PythonOperator(
        task_id='collect_tweets',
        python_callable=collect_tweets,
    )

    # Задача 2: Обработка на туитове от Kafka
    process_task = PythonOperator(
        task_id='process_tweets',
        python_callable=process_tweets,
    )

    # Задача 3: Анализ на туитове
    analyze_task = PythonOperator(
        task_id='analyze_tweets',
        python_callable=analyze_tweets,
    )

    # Задача 4: Генериране на отчет
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    # Дефиниране на последователност
    collect_task >> process_task >> analyze_task >> report_task
