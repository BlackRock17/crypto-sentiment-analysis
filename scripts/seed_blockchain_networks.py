# scripts/seed_blockchain_networks.py
import logging
import sys
from pathlib import Path

# Добавяме основната директория на проекта към пътя
sys.path.append(str(Path(__file__).parent.parent))

from src.data_processing.database import get_db
from src.data_processing.crud.read import get_blockchain_network_by_name
from src.data_processing.crud.create import create_blockchain_network

# Конфигуриране на логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def seed_blockchain_networks():
    """Зареждане на основни блокчейн мрежи в базата данни."""
    db = next(get_db())
    try:
        # Дефиниране на основни мрежи
        networks = [
            {
                "name": "solana",
                "display_name": "Solana",
                "description": "Solana is a high-performance blockchain supporting builders around the world.",
                "hashtags": ["solana", "sol", "solanasummer", "solananft"],
                "keywords": ["solana", "sol", "spl"],
                "is_active": True
            },
            {
                "name": "ethereum",
                "display_name": "Ethereum",
                "description": "Ethereum is a decentralized, open-source blockchain with smart contract functionality.",
                "hashtags": ["ethereum", "eth", "erc20"],
                "keywords": ["ethereum", "eth", "erc20", "gwei", "wei"],
                "is_active": True
            },
            {
                "name": "binance",
                "display_name": "Binance Smart Chain",
                "description": "Binance Smart Chain is a blockchain network built for running smart contract-based applications.",
                "hashtags": ["bnb", "bsc", "binancesmartchain"],
                "keywords": ["binance", "bnb", "bsc", "bep20"],
                "is_active": True
            },
            {
                "name": "polygon",
                "display_name": "Polygon",
                "description": "Polygon is a protocol and a framework for building and connecting Ethereum-compatible blockchain networks.",
                "hashtags": ["polygon", "matic"],
                "keywords": ["polygon", "matic"],
                "is_active": True
            }
        ]

        # Вмъкване на мрежи, ако не съществуват
        networks_added = 0
        for network_data in networks:
            existing_network = get_blockchain_network_by_name(db, network_data["name"])
            if existing_network:
                logger.info(f"Мрежата '{network_data['name']}' вече съществува")
                continue

            network = create_blockchain_network(
                db=db,
                **network_data
            )
            networks_added += 1
            logger.info(f"Създадена мрежа: {network.name}")

        logger.info(f"Зареждането на блокчейн мрежи завършено. Добавени: {networks_added} мрежи.")

    except Exception as e:
        logger.error(f"Грешка при зареждане на блокчейн мрежи: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    seed_blockchain_networks()
