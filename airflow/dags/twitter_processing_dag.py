"""
DAG за обработка на Twitter данни за криптовалути.
"""
import os
import sys
import json
import logging
import time
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


def test_detailed_connection(**kwargs):
    """Подробна проверка на свързаността към Kafka"""
    logger.info("Започва детайлна проверка на свързаността с Kafka")

    try:
        # 1. TCP свързаност
        import socket
        hosts_to_check = ['kafka:9092', 'localhost:29092', 'kafka:29092']

        for host_port in hosts_to_check:
            host, port = host_port.split(':')
            port = int(port)

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            result = s.connect_ex((host, port))
            s.close()

            status = "УСПЕШНА" if result == 0 else f"НЕУСПЕШНА (код {result})"
            logger.info(f"TCP връзка към {host_port}: {status}")

        # 2. Проверка с AdminClient (ако е възможно)
        try:
            from confluent_kafka.admin import AdminClient

            # Пробвайка с различни конфигурации
            configs = [
                {'bootstrap.servers': 'kafka:9092'},
                {'bootstrap.servers': 'localhost:29092'},
                {'bootstrap.servers': 'kafka:29092'}
            ]

            for idx, config in enumerate(configs):
                try:
                    logger.info(f"Опит за AdminClient с конфигурация: {config}")
                    admin = AdminClient(config)
                    topics = admin.list_topics(timeout=10)
                    logger.info(
                        f"Успешно свързване с конфигурация #{idx + 1}! Намерени топици: {list(topics.topics.keys())}")
                except Exception as e:
                    logger.warning(f"AdminClient с конфигурация #{idx + 1} се провали: {e}")

        except ImportError:
            logger.error("Не може да се импортира confluent_kafka.admin.AdminClient")

        return True
    except Exception as e:
        logger.error(f"Грешка при тестване на свързаността: {e}")
        return False


def test_kafka_connectivity(**kwargs):
    """
    Тест за директна свързаност с Kafka
    """
    logger.info("Тестване на директна свързаност с Kafka")

    try:
        import socket

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        result = s.connect_ex(('kafka', 9092))
        s.close()

        if result == 0:
            logger.info("Успешна TCP връзка с Kafka на адрес kafka:9092")
        else:
            logger.error(f"Неуспешна TCP връзка с Kafka, код на грешка: {result}")

        # Проверка и с confluent клиент
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({'bootstrap.servers': 'kafka:9092'})
        topics = admin.list_topics(timeout=10)
        logger.info(f"Намерени топици в Kafka: {list(topics.topics.keys())}")

        return True
    except Exception as e:
        logger.error(f"Грешка при тестване на свързаността: {e}")
        return False


# Функция за събиране на туитове
def collect_tweets(**kwargs):
    """
    Събира примерни туитове и ги изпраща към Kafka.
    """
    logger.info("Започва събиране на примерни туитове")

    try:
        # Създаваме примерни туитове с уникални идентификатори
        current_timestamp = int(datetime.now().timestamp())
        sample_tweets = [
            {
                "tweet_id": f"airflow_test_{current_timestamp}_1",
                "text": "Това е тестов туит от Airflow за $SOL и $BTC! #Тест",
                "author_username": "airflow_tester",
                "author_id": "airflow_test_user",
                "created_at": datetime.now().isoformat(),
                "retweet_count": 42,
                "like_count": 100
            },
            {
                "tweet_id": f"airflow_test_{current_timestamp}_2",
                "text": "Втори тестов туит за $ETH и $SOL криптовалути! #Airflow #Тест",
                "author_username": "crypto_enthusiast",
                "author_id": "crypto_test_user",
                "created_at": datetime.now().isoformat(),
                "retweet_count": 24,
                "like_count": 78
            }
        ]

        # Сериализираме туитовете за запазване в XCom
        from copy import deepcopy
        # Запазваме туитовете в XCom (за всеки случай)
        kwargs['ti'].xcom_push(key='sample_tweets', value=deepcopy(sample_tweets))

        # Директно използване на confluent-kafka Producer
        from confluent_kafka import Producer

        # Използваме успешния адрес от тестовете
        producer_config = {
            'bootstrap.servers': 'kafka:9092',  # Потвърден работещ адрес
            'client.id': 'airflow-twitter-producer'
        }

        producer = Producer(producer_config)
        logger.info(f"Създаден Kafka продуцент с конфигурация: {producer_config}")

        # Callback функция за доставка
        def delivery_callback(err, msg):
            if err:
                logger.error(f"Грешка при доставяне на съобщение: {err}")
            else:
                topic = msg.topic()
                partition = msg.partition()
                offset = msg.offset() if msg.offset() is not None else -1
                key = msg.key().decode('utf-8') if msg.key() else "None"
                logger.info(f"Съобщението успешно доставено до {topic} [part:{partition}, offset:{offset}], key={key}")

        # Изпращаме туитовете
        success_count = 0
        for tweet in sample_tweets:
            try:
                # Сериализиране на JSON
                tweet_json = json.dumps(tweet).encode('utf-8')
                tweet_key = str(tweet['tweet_id']).encode('utf-8')

                # Директно изпращане
                producer.produce(
                    topic='twitter-raw-tweets',
                    value=tweet_json,
                    key=tweet_key,
                    callback=delivery_callback
                )

                # Обработка на callbacks
                producer.poll(0)

                logger.info(f"Поставено в опашката за изпращане: туит {tweet['tweet_id']}")
                success_count += 1
            except Exception as e:
                logger.error(f"Грешка при изпращане на туит {tweet['tweet_id']}: {str(e)}")

        # Изчакване за изпращане на всички съобщения (по-дълъг timeout)
        logger.info("Изчакване за изпращане на всички съобщения...")
        remaining = producer.flush(timeout=30.0)

        if remaining > 0:
            logger.warning(f"{remaining} съобщения не бяха доставени в рамките на timeout периода")
        else:
            logger.info("Всички съобщения бяха успешно доставени до Kafka")

        # Малко изчакване
        import time
        logger.info("Изчакване 3 секунди за гарантиране, че съобщенията са налични...")
        time.sleep(3)

        logger.info(f"Събрани и изпратени {success_count} туита към Kafka")
        return success_count

    except Exception as e:
        logger.error(f"Грешка при събиране на туитове: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def process_tweets(**kwargs):
    """
    Обработка на туитове от Kafka.
    """
    logger.info("Започва обработка на туитове от Kafka")

    try:
        # Импортираме директно от src
        from confluent_kafka import Consumer, KafkaError

        # Конфигурация на консуматора с проверен адрес
        config = {
            'bootstrap.servers': 'kafka:9092',  # Потвърден работещ адрес
            'group.id': f'airflow-twitter-processor-{int(datetime.now().timestamp())}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
            'request.timeout.ms': 30000,
        }

        logger.info(f"Създаване на консуматор с конфигурация: {config}")

        # Създаване на консуматор
        consumer = Consumer(config)
        logger.info("Създаден консуматор за twitter-raw-tweets")

        # Абониране за топика с туитове
        consumer.subscribe(['twitter-raw-tweets'])
        logger.info("Абониран за twitter-raw-tweets")

        # Списък за съхранение на обработени туитове
        processed_tweets = []

        # Туитове с airflow_test префикс (тези, които ние генерираме)
        airflow_tweets = []

        # Специални идентификатори за следене
        ti = kwargs['ti']
        sample_tweets = ti.xcom_pull(task_ids='collect_tweets', key='sample_tweets')
        target_ids = []
        if sample_tweets:
            target_ids = [tweet['tweet_id'] for tweet in sample_tweets]
            logger.info(f"Търсим туитове с ID: {target_ids}")

        # Опит за получаване на съобщения (с таймаут)
        start_time = datetime.now()
        timeout_seconds = 60  # 1 минута
        end_time = start_time + timedelta(seconds=timeout_seconds)

        logger.info(f"Започваме да четем съобщения от {start_time} до {end_time}")

        while datetime.now() < end_time:
            msg = consumer.poll(timeout=1.0)

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
                try:
                    # Информация за съобщението
                    topic = msg.topic()
                    partition = msg.partition()
                    offset = msg.offset()
                    key = msg.key().decode('utf-8') if msg.key() else "None"

                    logger.info(f"Получено съобщение от {topic} [part:{partition}, offset:{offset}, key:{key}]")

                    # Декодиране на стойността
                    msg_value = msg.value().decode('utf-8')
                    tweet_data = json.loads(msg_value)

                    # Показване на част от съдържанието
                    preview = msg_value[:100] + "..." if len(msg_value) > 100 else msg_value
                    logger.info(f"Съдържание на съобщението: {preview}")

                    # Проверка дали е от нашите туитове
                    tweet_id = tweet_data.get('tweet_id', '')

                    if tweet_id in target_ids:
                        logger.info(f"НАМЕРЕН е един от нашите целеви туитове: {tweet_id}")

                    if tweet_id.startswith('airflow_test_'):
                        logger.info(f"Намерен е туит с нашия префикс: {tweet_id}")
                        airflow_tweets.append(tweet_data)

                    # Обработка на съобщението
                    processed_tweet = {
                        'tweet_id': tweet_id,
                        'text': tweet_data.get('text', ''),
                        'author': tweet_data.get('author_username', ''),
                        'processed_at': datetime.now().isoformat()
                    }

                    processed_tweets.append(processed_tweet)
                    logger.info(f"Обработен туит: {processed_tweet['tweet_id']}")

                except Exception as e:
                    logger.error(f"Грешка при обработка на съобщение: {e}")
                    import traceback
                    logger.error(traceback.format_exc())

        # Затваряне на консуматора
        consumer.close()

        # Статистика и логове
        logger.info(f"Общо обработени туитове: {len(processed_tweets)}")
        logger.info(f"Туитове с airflow_test префикс: {len(airflow_tweets)}")

        # Ако нямаме нищо от Kafka, използваме тези от XCom
        if not processed_tweets and sample_tweets:
            logger.info("Не са намерени туитове от Kafka, използваме тези от XCom")
            for tweet in sample_tweets:
                processed_tweet = {
                    'tweet_id': tweet.get('tweet_id'),
                    'text': tweet.get('text'),
                    'author': tweet.get('author_username'),
                    'processed_at': datetime.now().isoformat()
                }
                processed_tweets.append(processed_tweet)

            logger.info(f"Добавени {len(processed_tweets)} туита от XCom")

        # Запазване на резултатите за следващата задача
        kwargs['ti'].xcom_push(key='processed_tweets', value=processed_tweets)
        kwargs['ti'].xcom_push(key='airflow_tweets', value=airflow_tweets)

        return len(processed_tweets)

    except Exception as e:
        logger.error(f"Грешка при обработка на туитове: {e}")
        import traceback
        logger.error(traceback.format_exc())

        # Резервен вариант - използваме туитовете от XCom
        try:
            ti = kwargs['ti']
            sample_tweets = ti.xcom_pull(task_ids='collect_tweets', key='sample_tweets')

            if sample_tweets:
                processed_tweets = []
                for tweet in sample_tweets:
                    processed_tweet = {
                        'tweet_id': tweet.get('tweet_id'),
                        'text': tweet.get('text'),
                        'author': tweet.get('author_username'),
                        'processed_at': datetime.now().isoformat()
                    }
                    processed_tweets.append(processed_tweet)

                logger.info(f"Резервен вариант: използвани {len(processed_tweets)} туита от XCom")

                # Запазване на резултатите
                kwargs['ti'].xcom_push(key='processed_tweets', value=processed_tweets)
                return len(processed_tweets)
        except Exception as fallback_error:
            logger.error(f"Грешка в резервния вариант: {fallback_error}")

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
    test_connection_detailed = PythonOperator(
        task_id='test_detailed_connection',
        python_callable=test_detailed_connection,
    )

    test_connectivity_task = PythonOperator(
        task_id='test_kafka_connectivity',
        python_callable=test_kafka_connectivity,
    )

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
    test_connection_detailed >> test_connectivity_task >> collect_task >> process_task >> analyze_task >> report_task
