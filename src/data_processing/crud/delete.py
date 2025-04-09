from sqlalchemy.orm import Session

from src.data_processing.models.database import (
    Tweet, Token, Network, MarketSentiment,
    TokenSentiment, NetworkSentiment, Influencer
)


def delete_tweet(
        db: Session,
        tweet_id: int
) -> bool:

    db_tweet = db.query(Tweet).filter(Tweet.id == tweet_id).first()

    if db_tweet is None:
        return False

    db.delete(db_tweet)
    db.commit()

    return True


def delete_tweet_by_twitter_id(
        db: Session,
        twitter_id: str
) -> bool:

    db_tweet = db.query(Tweet).filter(Tweet.tweet_id == twitter_id).first()

    if db_tweet is None:
        return False

    return delete_tweet(db=db, tweet_id=db_tweet.id)


def delete_token(
        db: Session,
        token_id: int
) -> bool:

    db_token = db.query(Token).filter(Token.id == token_id).first()

    if db_token is None:
        return False

    db.delete(db_token)
    db.commit()

    return True


def delete_token_by_symbol(
        db: Session,
        symbol: str
) -> bool:

    db_token = db.query(Token).filter(Token.symbol == symbol).first()

    if db_token is None:
        return False

    return delete_token(db=db, token_id=db_token.id)


def delete_network(
        db: Session,
        network_id: int
) -> bool:

    db_network = db.query(Network).filter(Network.id == network_id).first()

    if db_network is None:
        return False

    db.delete(db_network)
    db.commit()

    return True


def delete_network_by_name(
        db: Session,
        name: str
) -> bool:

    db_network = db.query(Network).filter(Network.name == name).first()

    if db_network is None:
        return False

    return delete_network(db=db, network_id=db_network.id)


def delete_market_sentiment(
        db: Session,
        sentiment_id: int
) -> bool:

    db_sentiment = db.query(MarketSentiment).filter(MarketSentiment.id == sentiment_id).first()

    if db_sentiment is None:
        return False

    db.delete(db_sentiment)
    db.commit()

    return True


def delete_market_sentiment_by_tweet_id(
        db: Session,
        tweet_id: int
) -> bool:

    db_sentiment = db.query(MarketSentiment).filter(MarketSentiment.tweet_id == tweet_id).first()

    if db_sentiment is None:
        return False

    db.delete(db_sentiment)
    db.commit()

    return True


def delete_token_sentiment(
        db: Session,
        sentiment_id: int
) -> bool:

    db_sentiment = db.query(TokenSentiment).filter(TokenSentiment.id == sentiment_id).first()

    if db_sentiment is None:
        return False

    db.delete(db_sentiment)
    db.commit()

    return True


def delete_token_sentiments_by_token_id(
        db: Session,
        token_id: int
) -> int:

    result = db.query(TokenSentiment).filter(TokenSentiment.token_id == token_id).delete()
    db.commit()

    return result


def delete_token_sentiments_by_tweet_id(
        db: Session,
        tweet_id: int
) -> int:

    result = db.query(TokenSentiment).filter(TokenSentiment.tweet_id == tweet_id).delete()
    db.commit()

    return result


def delete_network_sentiment(
        db: Session,
        sentiment_id: int
) -> bool:

    db_sentiment = db.query(NetworkSentiment).filter(NetworkSentiment.id == sentiment_id).first()

    if db_sentiment is None:
        return False

    db.delete(db_sentiment)
    db.commit()

    return True


def delete_network_sentiments_by_network_id(
        db: Session,
        network_id: int
) -> int:

    result = db.query(NetworkSentiment).filter(NetworkSentiment.network_id == network_id).delete()
    db.commit()

    return result


def delete_network_sentiments_by_tweet_id(
        db: Session,
        tweet_id: int
) -> int:

    result = db.query(NetworkSentiment).filter(NetworkSentiment.tweet_id == tweet_id).delete()
    db.commit()

    return result


def delete_influencer(
        db: Session,
        influencer_id: int
) -> bool:

    db_influencer = db.query(Influencer).filter(Influencer.id == influencer_id).first()

    if db_influencer is None:
        return False

    db.delete(db_influencer)
    db.commit()

    return True


def delete_influencer_by_username(
        db: Session,
        username: str
) -> bool:

    db_influencer = db.query(Influencer).filter(Influencer.username == username).first()

    if db_influencer is None:
        return False

    return delete_influencer(db=db, influencer_id=db_influencer.id)
