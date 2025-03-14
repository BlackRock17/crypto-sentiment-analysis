"""
Конфигурация на логването за проекта.
Запиши този файл в основната директория на проекта.
"""
import logging
import sys
from pathlib import Path


def setup_logging(level=logging.DEBUG):
    """
    Настройва логването за целия проект.

    Args:
        level: Ниво на логване (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Създаваме директория за логове ако не съществува
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Основен формат за логване
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Конфигуриране на root logger
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            # Логване на конзолата
            logging.StreamHandler(sys.stdout),
            # Логване във файл
            logging.FileHandler(logs_dir / "debug.log", mode="w")
        ]
    )

    # Намаляваме нивото на логване за някои шумни библиотеки
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)

    # Увеличаваме нивото на логване за нашите компоненти
    logging.getLogger("src.data_processing.kafka.consumers").setLevel(logging.DEBUG)
    logging.getLogger("src.data_collection.twitter").setLevel(logging.DEBUG)

    logging.info("Logging configured successfully")


if __name__ == "__main__":
    setup_logging()
    logging.info("Test log message")
    