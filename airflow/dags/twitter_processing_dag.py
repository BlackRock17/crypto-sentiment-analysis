"""
DAG за обработка на Twitter данни за криптовалути.
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
# Airflow импорти
from airflow import DAG
from airflow.operators.python import PythonOperator

# Уверете се, че пътят към src директорията е добавен
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
SRC_DIR = os.path.join(AIRFLOW_HOME, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Конфигурация на логера
logger = logging.getLogger(__name__)

# Аргументи по подразбиране за DAG
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
    """
    Събира примерни туитове и ги изпраща към Kafka.
    """
    logger.info("Започва събиране на примерни туитове")

    try:
        # Импортираме директно от src
        from src.data_processing.kafka.producer import TwitterProducer

        # Създаваме TwitterProducer
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


def process_tweets(**kwargs):
    """
    Обработка на туитове от Kafka.
    """
    logger.info("Започва обработка на туитове от Kafka")

    try:
        # Импортираме директно от src
        from confluent_kafka import Consumer, KafkaError
        from src.data_processing.kafka.config import TOPICS

        # Проверка на връзката с Kafka
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({'bootstrap.servers': 'kafka:9092'})  # Променен адрес
            metadata = admin.list_topics(timeout=10)
            logger.info(f"Успешна връзка с Kafka! Налични топици: {list(metadata.topics.keys())}")
        except Exception as e:
            logger.error(f"Грешка при свързване с Kafka: {e}")
            raise

        # Конфигурация на консуматора с повече детайли
        config = {
            'bootstrap.servers': 'kafka:9092',  # Променен адрес
            'group.id': 'airflow-twitter-processor',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,  # По-висок таймаут
            'request.timeout.ms': 30000,  # По-висок таймаут
        }

        logger.info(f"Ще четем от топик: {TOPICS['RAW_TWEETS']}")
        logger.info(f"Consumer конфигурация: {config}")

        # Създаване на консуматор
        consumer = Consumer(config)
        logger.info("Свързан успешно с Kafka брокера, ще търся съобщения в топик: %s", TOPICS['RAW_TWEETS'])

        # Абониране за топика с туитове
        consumer.subscribe([TOPICS['RAW_TWEETS']])

        # Списък за съхранение на обработени туитове
        processed_tweets = []

        # Опит за получаване на съобщения (с таймаут)
        start_time = datetime.now()
        timeout_seconds = 60
        end_time = start_time + timedelta(seconds=timeout_seconds)

        logger.info(f"Започваме да четем съобщения от {start_time} до {end_time}")

        while datetime.now() < end_time:
            msg = consumer.poll(timeout=10.0)

            if msg is None:
                logger.debug("Няма съобщения, продължаваме да чакаме...")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Достигнат е краят на партиция {msg.topic()}")
                else:
                    logger.error(f"Грешка при четене: {msg.error()}")
            else:
                # Декодиране и обработка на съобщението
                logger.info("Получено съобщение от Kafka: %s", msg.value().decode('utf-8')[:100] + "...")
                try:
                    tweet_data = json.loads(msg.value().decode('utf-8'))

                    if 'created_at' in tweet_data and isinstance(tweet_data['created_at'], str):
                        try:
                            tweet_data['created_at'] = datetime.fromisoformat(
                                tweet_data['created_at'].replace('Z', '+00:00'))
                        except ValueError:
                            # Игнорирайте грешки при parse-ване на датата
                            pass

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


def analyze_tweets(**kwargs):
    """
    Анализ на обработените туитове.
    """
    ti = kwargs['ti']

    # Вземане на обработените туитове от предишната задача
    processed_tweets = ti.xcom_pull(task_ids='process_tweets', key='processed_tweets')

    if not processed_tweets:
        logger.warning("Няма обработени туитове за анализ")
        # Създаваме тестови данни, когато няма реални туитове
        processed_tweets = [
            {
                'tweet_id': 'test_id_1',
                'text': 'Тестов туит за $BTC и $ETH анализ',
                'author': 'test_user',
                'processed_at': datetime.now().isoformat()
            },
            {
                'tweet_id': 'test_id_2',
                'text': 'Друг тестов туит за $SOL #Solana',
                'author': 'test_user2',
                'processed_at': datetime.now().isoformat()
            }
        ]
        logger.info("Използвайки тестови данни за демонстрация")

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


def generate_report(**kwargs):
    """
    Генериране на отчет.
    """
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
