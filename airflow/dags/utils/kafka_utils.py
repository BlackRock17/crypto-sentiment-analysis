"""
Помощни функции за работа с Kafka в Airflow DAG-ове.
Включва функции за проверка на свързаността, състоянието на топиците и други диагностики.
"""
import logging
import socket
import time
from typing import Dict, Any, List, Optional, Tuple, Set

# Конфигуриране на логер
logger = logging.getLogger(__name__)


def test_kafka_detailed() -> Dict[str, bool]:
    """
    Извършва подробна проверка на свързаността с Kafka, включително различни
    адреси и конфигурации.

    Returns:
        Dict[str, bool]: Речник с резултати от проверките
    """
    results = {}

    # 1. Проверка на TCP свързаност към различни адреси за Kafka
    hosts_to_check = [
        ('kafka', 9092),  # Вътрешен адрес в Docker мрежата
        ('kafka', 29092),  # Алтернативен порт
        ('localhost', 29092),  # Адрес през host мрежата
    ]

    for host, port in hosts_to_check:
        test_name = f"TCP връзка към {host}:{port}"
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            result = s.connect_ex((host, port))
            s.close()

            results[test_name] = (result == 0)

            status = "УСПЕШНА" if result == 0 else f"НЕУСПЕШНА (код {result})"
            logger.info(f"TCP връзка към {host}:{port}: {status}")
        except Exception as e:
            results[test_name] = False
            logger.error(f"Грешка при свързване с {host}:{port}: {e}")

    # 2. Проверка чрез AdminClient (ако е наличен)
    try:
        from confluent_kafka.admin import AdminClient

        # Опитай различни конфигурации
        configs = [
            {'bootstrap.servers': 'kafka:9092'},
            {'bootstrap.servers': 'localhost:29092'},
            {'bootstrap.servers': 'kafka:29092'}
        ]

        for idx, config in enumerate(configs):
            test_name = f"AdminClient с конфигурация {config}"

            try:
                admin = AdminClient(config)
                topics = admin.list_topics(timeout=10)

                results[test_name] = True

                topic_list = list(topics.topics.keys())
                logger.info(
                    f"Успешно свързване с конфигурация {config}! "
                    f"Намерени топици: {topic_list}"
                )
            except Exception as e:
                results[test_name] = False
                logger.warning(f"AdminClient с конфигурация {config} се провали: {e}")

    except ImportError:
        logger.error("Не може да се импортира confluent_kafka.admin.AdminClient")
        results["AdminClient импортиране"] = False

    return results


def check_kafka_topics_health() -> Dict[str, Any]:
    """
    Проверява състоянието на Kafka топиците, техните партиции
    и достъпността им.

    Returns:
        Dict[str, Any]: Информация за състоянието на топиците
    """
    from kafka_config import TOPICS, DEFAULT_BOOTSTRAP_SERVERS

    # Резултати от проверката
    results = {
        'topics': {},
        'all_topics_healthy': True
    }

    try:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka import Consumer

        # Създаване на AdminClient за проверка на топиците
        admin_client = AdminClient({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})

        # Получаване на метаданни за всички топици
        kafka_metadata = admin_client.list_topics(timeout=10)

        # Проверка дали всички нужни топици съществуват
        required_topics = set(TOPICS.values())
        existing_topics = set(kafka_metadata.topics.keys())

        missing_topics = required_topics - existing_topics

        if missing_topics:
            logger.warning(f"Липсващи топици: {missing_topics}")
            results['all_topics_healthy'] = False

        # Създаване на консуматор за проверка на съобщенията
        consumer_config = {
            'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS,
            'group.id': f'airflow-kafka-health-checker-{int(time.time())}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 10000
        }

        consumer = Consumer(consumer_config)

        # Проверка на всеки топик
        for topic_name, topic_metadata in kafka_metadata.topics.items():
            # Пропускаме вътрешните топици
            if topic_name.startswith('__'):
                continue

            # Събиране на информация за топика
            partition_count = len(topic_metadata.partitions)
            is_healthy = True
            message_count = 0

            # Проверка на всяка партиция
            for partition_id, partition_metadata in topic_metadata.partitions.items():
                # Проверка за наличие на лидер
                if partition_metadata.leader < 0:
                    logger.warning(f"Топик {topic_name}, партиция {partition_id} няма лидер!")
                    is_healthy = False

            # Опит за получаване на приблизителен брой съобщения
            try:
                consumer.subscribe([topic_name])

                # Изчакване за получаване на метаданни
                for _ in range(5):
                    consumer.poll(timeout=1.0)

                    # Получаване на водаческите партиции
                    assignment = consumer.assignment()

                    if assignment:
                        # Получаване на позициите на партициите
                        for tp in assignment:
                            # Отместване в края
                            high_offset = consumer.get_watermark_offsets(tp)[1]
                            message_count += high_offset

                        # Прекратяване след получаване на информацията
                        break

                consumer.unsubscribe()

            except Exception as e:
                logger.error(f"Грешка при проверка на съобщенията в топик {topic_name}: {e}")
                is_healthy = False

            # Записване на резултатите
            results['topics'][topic_name] = {
                'partition_count': partition_count,
                'message_count': message_count,
                'is_healthy': is_healthy
            }

            # Ако някой топик не е здрав, отбелязваме това в общия резултат
            if not is_healthy:
                results['all_topics_healthy'] = False

        # Затваряне на консуматора
        consumer.close()

    except Exception as e:
        logger.error(f"Грешка при проверка на Kafka топиците: {e}")
        results['error'] = str(e)
        results['all_topics_healthy'] = False

    return results


def read_sample_messages(topic_name: str, max_messages: int = 5) -> List[Dict[str, Any]]:
    """
    Прочита примерни съобщения от избран Kafka топик.
    Полезно за диагностика и мониторинг на данните.

    Args:
        topic_name: Име на топика
        max_messages: Максимален брой съобщения за прочитане

    Returns:
        List[Dict[str, Any]]: Списък с прочетени съобщения
    """
    from src.data_processing.kafka.config import DEFAULT_BOOTSTRAP_SERVERS

    messages = []

    try:
        from confluent_kafka import Consumer, KafkaError
        import json

        # Конфигурация на консуматора
        config = {
            'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS,
            'group.id': f'airflow-message-reader-{int(time.time())}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 10000
        }

        # Създаване на консуматор
        consumer = Consumer(config)

        # Абониране за топика
        consumer.subscribe([topic_name])

        # Опит за получаване на съобщения (с таймаут)
        for _ in range(max_messages * 2):  # Опитваме двойно повече пъти за сигурност
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Достигнат е краят на партиция {msg.topic()}")
                else:
                    logger.error(f"Грешка при четене: {msg.error()}")
            else:
                # Декодиране на съобщението
                try:
                    value = msg.value().decode('utf-8')

                    # Опит за декодиране на JSON
                    try:
                        data = json.loads(value)
                    except json.JSONDecodeError:
                        data = {"raw_text": value}

                    # Добавяне на метаданни
                    data["_kafka_metadata"] = {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": msg.timestamp()[1] if msg.timestamp() else None
                    }

                    messages.append(data)

                    # Прекратяване, когато достигнем лимита
                    if len(messages) >= max_messages:
                        break

                except Exception as e:
                    logger.error(f"Грешка при декодиране на съобщение: {e}")

        # Затваряне на консуматора
        consumer.close()

    except Exception as e:
        logger.error(f"Грешка при четене на съобщения от топик {topic_name}: {e}")

    return messages
