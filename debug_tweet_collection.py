"""
Скрипт за дебъгване на автоматичното събиране на туитове.
Този скрипт позволява да тестваш цялата логика на автоматичното събиране
на туитове в изолирана среда без да е необходимо да стартираш целия API сървър.

Инструкции:
1. Постави този файл в основната директория на проекта
2. Добави твоите точки за прекъсване (breakpoints) в съответните файлове
3. Стартирай този скрипт в режим debug
"""
import asyncio
import logging
import os
import sys
from datetime import datetime

# Настройване на logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Добавяне на базовата директория към пътя, за да можем да импортираме пакетите
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

# Настройване на тестов режим
os.environ['TESTING'] = 'true'

# Импортиране на необходимите модули
from src.data_processing.database import get_db
from src.data_collection.twitter.config import twitter_config
from src.data_collection.twitter.client import TwitterAPIClient
from src.data_collection.twitter.service import TwitterCollectionService
from src.data_collection.tasks.twitter_tasks import collect_automated_tweets, _async_collect_automated_tweets
from src.data_processing.models.twitter import TwitterInfluencer
from src.data_processing.crud.twitter import create_influencer, get_influencer_by_username, get_all_influencers


# Функция за проверка и създаване на тестови инфлуенсъри
async def ensure_test_influencers():
    """
    Проверява и създава тестови инфлуенсъри, ако не съществуват.
    """
    logger.info("Проверка за тестови инфлуенсъри...")

    # Получаване на сесия към базата данни
    db = next(get_db())

    try:
        # Списък с тестови инфлуенсъри
        test_influencers = [
            {"username": "crypto_expert", "name": "Crypto Expert", "is_automated": True},
            {"username": "blockchain_news", "name": "Blockchain News", "is_automated": True},
            {"username": "coin_analyst", "name": "Coin Analyst", "is_automated": True}
        ]

        for influencer_data in test_influencers:
            username = influencer_data["username"]

            # Проверка дали инфлуенсърът вече съществува
            existing = get_influencer_by_username(db, username)

            if not existing:
                logger.info(f"Създаване на тестов инфлуенсър: {username}")
                create_influencer(
                    db=db,
                    username=username,
                    name=influencer_data.get("name", username),
                    description=f"Test influencer for {username}",
                    follower_count=1000,
                    is_active=True,
                    is_automated=influencer_data.get("is_automated", False),
                    priority=1
                )
            else:
                logger.info(f"Инфлуенсърът {username} вече съществува")

                # Ако инфлуенсърът съществува, но не е автоматизиран, настрой го
                if influencer_data.get("is_automated", False) and not existing.is_automated:
                    logger.info(f"Настройване на {username} като автоматизиран")
                    existing.is_automated = True
                    db.commit()

        # Извеждане на всички инфлуенсъри
        all_influencers = get_all_influencers(db)
        logger.info(f"Общо инфлуенсъри в базата данни: {len(all_influencers)}")
        logger.info(f"Автоматизирани инфлуенсъри: {sum(1 for i in all_influencers if i.is_automated)}")

    finally:
        db.close()


# Функция за директно тестване на TwitterCollectionService
async def run_collection_service():
    """
    Тества директно TwitterCollectionService.
    """
    logger.info("Тестване на TwitterCollectionService...")

    # Получаване на сесия към базата данни
    db = next(get_db())

    try:
        # Настройване на тестов режим
        twitter_config.is_test_mode = True

        # Създаване на сървис
        service = TwitterCollectionService(db)

        # Тестване на връзката
        connection_ok = service.test_twitter_connection()
        logger.info(f"Twitter връзка: {'OK' if connection_ok else 'Грешка'}")

        # Събиране на туитове
        tweets_collected, mentions_found = service.collect_and_store_automated_tweets()
        logger.info(f"Събрани туитове: {tweets_collected}, открити споменавания: {mentions_found}")

        return tweets_collected

    finally:
        db.close()


# Функция за тестване чрез task функциите
async def run_collection_tasks():
    """
    Тества логиката чрез задачите в twitter_tasks.py.
    """
    logger.info("Тестване на collect_automated_tweets...")

    # Настройване на тестов режим
    twitter_config.is_test_mode = True

    # Събиране на туитове директно
    success = await _async_collect_automated_tweets()
    logger.info(f"Резултат от _async_collect_automated_tweets: {'Успех' if success else 'Грешка'}")

    # Събиране на туитове чрез синхронната обвивка
    success = collect_automated_tweets()
    logger.info(f"Резултат от collect_automated_tweets: {'Успех' if success else 'Грешка'}")

    return success


# Главна функция
async def main():
    logger.info("Стартиране на дебъг скрипта за събиране на туитове...")

    try:
        # Проверка и създаване на тестови инфлуенсъри
        await ensure_test_influencers()

        # Тестване на TwitterCollectionService
        tweets_collected = await run_collection_service()

        # Тестване на задачите
        success = await run_collection_tasks()

        logger.info("Дебъг сесията завърши успешно!")
        logger.info(f"Събрани туитове: {tweets_collected}")
        logger.info(f"Статус на задачите: {'Успех' if success else 'Грешка'}")

    except Exception as e:
        logger.error(f"Грешка при дебъгване: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    # За работа с асинхронни функции, създаваме event loop
    asyncio.run(main())
