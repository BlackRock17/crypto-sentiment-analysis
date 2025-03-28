"""
Помощни функции за работа с базата данни в Airflow DAG-ове.
Включва функции за проверка на свързаността, състоянието на таблиците и други диагностики.
"""
import logging
import os
from typing import Dict, Any, List, Optional, Tuple

# Конфигуриране на логер
logger = logging.getLogger(__name__)


def get_connection_params() -> Dict[str, Any]:
    """
    Взема параметрите за връзка с базата данни от средата.

    Returns:
        Dict[str, Any]: Речник с параметри за връзка
    """
    # Използваме PROJECT_DATABASE_URL, който е в docker-compose-airflow.yml файл
    DATABASE_URL = os.getenv('PROJECT_DATABASE_URL',
                             'postgresql://postgres:password@host.docker.internal:5432/crypto_sentiment')

    # Разбиване на URL на компоненти
    if 'postgresql://' in DATABASE_URL:
        url_parts = DATABASE_URL.replace('postgresql://', '').split('@')
        user_pass = url_parts[0].split(':')
        host_port_db = url_parts[1].split('/')
        host_port = host_port_db[0].split(':')

        params = {
            'user': user_pass[0],
            'password': user_pass[1],
            'host': host_port[0],
            'port': host_port[1] if len(host_port) > 1 else '5432',
            'database': host_port_db[1]
        }

        return params
    else:
        # Фолбек към хардкоднати настройки
        return {
            'user': 'postgres',
            'password': 'password',
            'host': 'host.docker.internal',
            'port': '5432',
            'database': 'crypto_sentiment'
        }


def check_database_health() -> Dict[str, Any]:
    """
    Проверява състоянието на базата данни, включително наличност на таблиците
    и статистика за броя редове.

    Returns:
        Dict[str, Any]: Информация за състоянието на базата данни
    """
    results = {
        'is_healthy': True,
        'tables': [],
        'row_counts': {},
        'message': 'Базата данни е в добро състояние'
    }

    try:
        import psycopg2
        conn_params = get_connection_params()

        # Свързване с базата данни
        conn = psycopg2.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            user=conn_params['user'],
            password=conn_params['password'],
            database=conn_params['database']
        )

        cursor = conn.cursor()

        # Проверка на PostgreSQL версията
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        results['version'] = version[0]

        # Вземане на списъка с таблици
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)

        tables = [row[0] for row in cursor.fetchall()]
        results['tables'] = tables

        # Основни таблици за проверка
        main_tables = [
            'tweets',
            'blockchain_tokens',
            'token_mentions',
            'sentiment_analysis',
            'twitter_influencers',
            'users'
        ]

        # Проверка на броя редове в основните таблици
        for table in main_tables:
            if table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table};")
                    count = cursor.fetchone()[0]
                    results['row_counts'][table] = count
                except Exception as e:
                    logger.error(f"Грешка при броене на редовете в таблица {table}: {e}")
                    results['row_counts'][table] = -1

        # Проверка на времената на последните записи
        if 'tweets' in tables:
            try:
                cursor.execute("""
                    SELECT MAX(created_at)
                    FROM tweets;
                """)
                latest_tweet = cursor.fetchone()[0]
                results['latest_tweet'] = latest_tweet.isoformat() if latest_tweet else None
            except Exception as e:
                logger.error(f"Грешка при проверка на последния туит: {e}")

        # Затваряне на връзката
        cursor.close()
        conn.close()

        # Проверка дали основните таблици съществуват
        missing_tables = [table for table in main_tables if table not in tables]
        if missing_tables:
            results['is_healthy'] = False
            results['message'] = f"Липсващи основни таблици: {', '.join(missing_tables)}"

    except Exception as e:
        results['is_healthy'] = False
        results['message'] = f"Грешка при проверка на базата данни: {str(e)}"
        logger.error(f"Грешка при проверка на базата данни: {e}")

    return results


def get_table_stats(table_name: str) -> Dict[str, Any]:
    """
    Получава детайлна статистика за конкретна таблица.

    Args:
        table_name: Име на таблицата

    Returns:
        Dict[str, Any]: Статистика за таблицата
    """
    stats = {}

    try:
        import psycopg2
        conn_params = get_connection_params()

        # Свързване с базата данни
        conn = psycopg2.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            user=conn_params['user'],
            password=conn_params['password'],
            database=conn_params['database']
        )

        cursor = conn.cursor()

        # Брой редове
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        stats['row_count'] = cursor.fetchone()[0]

        # Информация за колоните
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """)

        columns = []
        for column_name, data_type in cursor.fetchall():
            columns.append({
                'name': column_name,
                'type': data_type
            })

        stats['columns'] = columns

        # Размер на таблицата
        cursor.execute(f"""
            SELECT pg_size_pretty(pg_total_relation_size('{table_name}'));
        """)
        stats['size'] = cursor.fetchone()[0]

        # Затваряне на връзката
        cursor.close()
        conn.close()

    except Exception as e:
        stats['error'] = str(e)
        logger.error(f"Грешка при получаване на статистика за таблица {table_name}: {e}")

    return stats


def execute_custom_query(query: str) -> List[Tuple]:
    """
    Изпълнява потребителска SQL заявка и връща резултатите.

    Args:
        query: SQL заявка за изпълнение

    Returns:
        List[Tuple]: Резултати от заявката
    """
    results = []

    try:
        import psycopg2
        conn_params = get_connection_params()

        # Свързване с базата данни
        conn = psycopg2.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            user=conn_params['user'],
            password=conn_params['password'],
            database=conn_params['database']
        )

        cursor = conn.cursor()

        # Изпълнение на заявката
        cursor.execute(query)

        # Получаване на имената на колоните
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

        # Добавяне на имената на колоните като първи елемент
        results.append(tuple(column_names))

        # Получаване на резултатите
        for row in cursor.fetchall():
            results.append(row)

        # Затваряне на връзката
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Грешка при изпълнение на заявка: {e}")
        # Връщане на грешката като резултат
        results = [("error",), (str(e),)]

    return results
