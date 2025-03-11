import time
import logging
import argparse
import random
from datetime import datetime

from src.data_processing.kafka.producer import TwitterProducer
from src.data_collection.twitter.config import twitter_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sample tweets with varying sentiment
SAMPLE_TWEETS = [
    # Positive tweets
    "I'm so excited about $SOL! The technology is amazing and the community is growing every day. #Solana #Crypto",
    "$ETH is the future of finance. Ethereum 2.0 will solve scaling issues and bring mass adoption. #Ethereum #Bullish",
    "Just bought more $BTC! Bitcoin is going to hit new all-time highs this year. #Bitcoin #ToTheMoon",

    # Negative tweets
    "Not sure about $SOL anymore. The recent outages are concerning. #Solana #CryptoNews",
    "$ETH fees are still too high. This needs to be fixed before mass adoption. #Ethereum #GasFees",
    "$BTC crashed again! This volatility is killing the market. #Bitcoin #Bearish",

    # Neutral tweets
    "$SOL is making progress on their roadmap. Interested to see how it evolves. #Solana",
    "Comparing $ETH and $SOL performance over the last quarter. Both have pros and cons. #Crypto",
    "$BTC market dominance is changing. Worth keeping an eye on altcoins. #Bitcoin #CryptoMarket"
]

# Sample usernames
SAMPLE_USERNAMES = [
    "crypto_lover",
    "blockchain_fan",
    "token_trader",
    "solana_developer",
    "eth_enthusiast",
    "bitcoin_maximalist"
]


def generate_tweet():
    """Generate a random tweet from the samples."""
    tweet_text = random.choice(SAMPLE_TWEETS)
    username = random.choice(SAMPLE_USERNAMES)

    return {
        'tweet_id': f"test_{int(time.time())}_{random.randint(1000, 9999)}",
        'text': tweet_text,
        'author_username': username,
        'author_id': f"user_{username}",
        'created_at': datetime.utcnow().isoformat(),
        'retweet_count': random.randint(0, 100),
        'like_count': random.randint(0, 500)
    }


def main():
    """Run the Kafka pipeline test."""
    parser = argparse.ArgumentParser(description='Test Kafka pipeline with sample tweets')
    parser.add_argument('--count', type=int, default=10, help='Number of test tweets to send')
    parser.add_argument('--interval', type=float, default=1.0, help='Interval between tweets in seconds')
    args = parser.parse_args()

    # Create producer
    producer = TwitterProducer()

    logger.info(f"Starting test: Sending {args.count} tweets at {args.interval}s intervals")

    # Send tweets
    for i in range(args.count):
        tweet = generate_tweet()
        logger.info(f"Sending tweet {i + 1}/{args.count}: {tweet['text'][:50]}...")

        success = producer.send_tweet(tweet)

        if success:
            logger.info(f"Tweet {i + 1} sent successfully")
        else:
            logger.error(f"Failed to send tweet {i + 1}")

        # Wait for next interval
        if i < args.count - 1:
            time.sleep(args.interval)

    # Flush to ensure all messages are delivered
    remaining = producer.flush(10)

    if remaining > 0:
        logger.warning(f"{remaining} messages could not be delivered within timeout")
    else:
        logger.info("All messages delivered successfully")

    logger.info("Test completed")


if __name__ == "__main__":
    # Check if Kafka credentials are valid
    if twitter_config.is_test_mode:
        logger.warning("Running in test mode - Kafka messages will be simulated")

    main()
