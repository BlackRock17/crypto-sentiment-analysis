"""
DAG за категоризация на токени.
Периодично стартира run_network_detection_batch функцията за автоматична категоризация
на токени с липсваща или ниско-доверителна blockchain мрежа.
"""
from datetime import datetime, timedelta
import logging
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Конфигуриране на логър
logger = logging.getLogger(__name__)

airflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /opt/airflow
sys.path.append(airflow_dir)

# # Добавяне на utils директорията към Python path
# dags_folder = os.path.dirname(os.path.abspath(__file__))
# utils_path = os.path.join(dags_folder, 'utils')
# sys.path.append(utils_path)
#
# # Добавяне на главната директория на проекта към Python path
# project_root = os.path.abspath(os.path.join(dags_folder, '..', '..'))
# sys.path.append(project_root)

# Стандартни аргументи за DAG-а
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=20),
}

# Създаване на DAG
with DAG(
        'token_categorization',
        default_args=default_args,
        description='Категоризиране на токени с липсваща или ниско-доверителна мрежа',
        schedule_interval=timedelta(days=1),  # Изпълнява се веднъж дневно
        catchup=False,
        tags=['tokens', 'categorization', 'blockchain']
) as dag:
    # Задача 1: Стартиране на run_network_detection_batch функцията
    def execute_network_detection_batch(**kwargs):
        """
        Извиква run_network_detection_batch функцията за категоризиране на токени.
        """
        try:
            # Импортиране на необходимите функции и модули
            from src.data_processing.database import get_db
            from src.data_processing.crud.core_queries import run_network_detection_batch

            # Параметри за функцията - могат да се конфигурират според нуждите
            min_mentions = 3  # Минимален брой споменавания на токена
            max_tokens = 100  # Максимален брой токени за обработка
            min_confidence = 0.0  # Минимално ниво на доверие за включване в резултатите

            logger.info(
                f"Стартиране на batch категоризация на токени с параметри: min_mentions={min_mentions}, max_tokens={max_tokens}, min_confidence={min_confidence}")

            # Получаване на сесия към базата данни
            db = next(get_db())

            try:
                # Изпълнение на run_network_detection_batch функцията
                results = run_network_detection_batch(
                    db=db,
                    min_mentions=min_mentions,
                    max_tokens=max_tokens,
                    min_confidence=min_confidence
                )

                # Запис на броя обработени токени
                tokens_processed = len(results)
                tokens_with_recommendation = sum(1 for r in results if r.get('recommended_network') is not None)
                tokens_with_high_confidence = sum(1 for r in results if
                                                  r.get('recommended_network') is not None and
                                                  r.get('confidence_score', 0) >= 0.7)

                logger.info(f"Batch категоризация завършена: обработени {tokens_processed} токени")
                logger.info(f"Токени с препоръчана мрежа: {tokens_with_recommendation}")
                logger.info(f"Токени с високо ниво на доверие (>= 0.7): {tokens_with_high_confidence}")

                # Връщане на резултатите за следващата задача
                return {
                    "tokens_processed": tokens_processed,
                    "tokens_with_recommendation": tokens_with_recommendation,
                    "tokens_with_high_confidence": tokens_with_high_confidence,
                    "execution_time": datetime.now().isoformat()
                }

            finally:
                # Затваряне на връзката с базата данни
                db.close()

        except Exception as e:
            logger.error(f"Грешка при изпълнение на batch категоризация: {e}")
            # Връщане на информация за грешката
            return {
                "error": str(e),
                "execution_time": datetime.now().isoformat(),
                "tokens_processed": 0
            }


    # Задача 2: Генериране на обобщен отчет
    def generate_summary_report(ti, **kwargs):
        """
        Генерира обобщен отчет за изпълнението на категоризацията на токени.
        """
        try:
            # Извличане на резултатите от предишната задача
            batch_results = ti.xcom_pull(task_ids='execute_network_detection_batch')

            if not batch_results:
                logger.warning("Не са намерени резултати от категоризацията")
                return {"status": "warning", "message": "Няма данни от категоризацията"}

            # Проверка за грешки
            if "error" in batch_results:
                error_message = batch_results.get("error", "Неизвестна грешка")
                logger.error(f"Категоризацията завърши с грешка: {error_message}")

                # Генериране на известие за грешка
                try:
                    from src.data_processing.database import get_db
                    from src.services.notification_service import NotificationService
                    from src.data_processing.models.notifications import NotificationPriority

                    db = next(get_db())
                    try:
                        notification_service = NotificationService(db)
                        notification_service.create_system_notification(
                            title="Грешка при категоризация на токени",
                            message=f"Процесът на категоризация завърши с грешка: {error_message}",
                            priority=NotificationPriority.HIGH
                        )
                    finally:
                        db.close()

                except Exception as notify_error:
                    logger.error(f"Грешка при създаване на известие: {notify_error}")

                return {"status": "error", "error": error_message}

            # Получаване на статистика
            tokens_processed = batch_results.get("tokens_processed", 0)
            tokens_with_recommendation = batch_results.get("tokens_with_recommendation", 0)
            tokens_with_high_confidence = batch_results.get("tokens_with_high_confidence", 0)
            execution_time = batch_results.get("execution_time", "unknown")

            # Създаване на обобщен отчет
            report = {
                "status": "success",
                "execution_time": execution_time,
                "tokens_processed": tokens_processed,
                "tokens_with_recommendation": tokens_with_recommendation,
                "tokens_with_high_confidence": tokens_with_high_confidence,
                "recommendation_rate": round((tokens_with_recommendation / tokens_processed * 100),
                                             2) if tokens_processed > 0 else 0,
                "high_confidence_rate": round((tokens_with_high_confidence / tokens_processed * 100),
                                              2) if tokens_processed > 0 else 0
            }

            # Логване на отчета
            logger.info(f"Отчет за категоризация на токени: {report}")

            # Генериране на известие с резултатите
            if tokens_processed > 0:
                try:
                    from src.data_processing.database import get_db
                    from src.services.notification_service import NotificationService
                    from src.data_processing.models.notifications import NotificationPriority

                    db = next(get_db())
                    try:
                        notification_service = NotificationService(db)
                        notification_service.create_system_notification(
                            title="Категоризация на токени завършена",
                            message=f"Обработени {tokens_processed} токени, {tokens_with_high_confidence} с високо ниво на доверие",
                            priority=NotificationPriority.MEDIUM,
                            metadata={
                                "tokens_processed": tokens_processed,
                                "tokens_with_recommendation": tokens_with_recommendation,
                                "tokens_with_high_confidence": tokens_with_high_confidence
                            }
                        )
                    finally:
                        db.close()

                except Exception as notify_error:
                    logger.error(f"Грешка при създаване на известие: {notify_error}")

            return report

        except Exception as e:
            logger.error(f"Грешка при генериране на отчет: {e}")
            return {"status": "error", "error": str(e)}


    # Дефиниране на задачите
    network_detection_task = PythonOperator(
        task_id='execute_network_detection_batch',
        python_callable=execute_network_detection_batch,
        provide_context=True
    )

    report_task = PythonOperator(
        task_id='generate_summary_report',
        python_callable=generate_summary_report,
        provide_context=True
    )

    # Задаване на зависимостите между задачите
    network_detection_task >> report_task
