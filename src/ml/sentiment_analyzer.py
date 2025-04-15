"""
Module for sentiment analysis of tweets using a pre-trained model.
"""
import logging
from typing import Dict, Tuple, Optional, List
import torch
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

from src.data_processing.models.database import SentimentEnum

# Set up logging
logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """
    Class for analyzing sentiment in text using a pre-trained DistilBERT model.
    """

    def __init__(self, model_name: str = "distilbert-base-uncased-finetuned-sst-2-english"):
        """
        Initialize the sentiment analyzer.

        Args:
            model_name: Name of the pre-trained model to use
        """
        self.model_name = model_name
        self.sentiment_pipeline = None
        self.initialized = False
        self.device = "cuda" if torch.cuda.is_available() else "cpu"

        # Try to initialize the model
        try:
            self._initialize_model()
        except Exception as e:
            logger.error(f"Error initializing model: {e}")
            logger.warning("Sentiment analyzer will initialize on first use")

    def _initialize_model(self):
        """
        Initialize the sentiment pipeline.
        """
        logger.info(f"Loading sentiment analysis model: {self.model_name}")
        try:
            # Load pre-trained model for sentiment classification
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model=self.model_name,
                tokenizer=self.model_name,
                device=self.device
            )
            self.initialized = True
            logger.info(f"Sentiment model loaded successfully (using {self.device})")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise

    def _ensure_initialized(self):
        """
        Ensure the model is initialized before use.
        """
        if not self.initialized:
            self._initialize_model()

    def analyze_text(self, text: str) -> Tuple[SentimentEnum, float]:
        """
        Analyze the sentiment of a given text.

        Args:
            text: The text to analyze

        Returns:
            Tuple of (sentiment, confidence_score)
        """
        try:
            self._ensure_initialized()

            # Check for empty text
            if not text or not text.strip():
                logger.warning("Attempted to analyze empty text")
                return SentimentEnum.NEUTRAL, 0.5

            # Perform prediction with the model
            result = self.sentiment_pipeline(text)[0]

            # Extract label and score
            label = result['label']
            score = result['score']

            # Map the result to our sentiment system
            sentiment = self._map_to_sentiment_enum(label, score)
            confidence = score

            # Log truncated text to avoid cluttering logs
            truncated_text = text[:50] + ('...' if len(text) > 50 else '')
            logger.info(f"Analyzed text: '{truncated_text}' -> {sentiment.value} ({confidence:.4f})")

            return sentiment, confidence

        except Exception as e:
            logger.error(f"Error during sentiment analysis: {e}")
            # Return neutral sentiment with low confidence on error
            return SentimentEnum.NEUTRAL, 0.5

    def _map_to_sentiment_enum(self, label: str, score: float) -> SentimentEnum:
        """
        Map the model output label to our SentimentEnum.

        Args:
            label: The label from the model (POSITIVE, NEGATIVE)
            score: The confidence score

        Returns:
            Corresponding SentimentEnum value
        """
        # Simple mapping based on the label
        if label.lower() == 'positive':
            return SentimentEnum.POSITIVE
        elif label.lower() == 'negative':
            return SentimentEnum.NEGATIVE
        else:
            # This should rarely happen with the default model
            return SentimentEnum.NEUTRAL

    def analyze_text_batch(self, texts: List[str]) -> List[Tuple[SentimentEnum, float]]:
        """
        Analyze sentiment for a batch of texts at once.

        Args:
            texts: List of texts to analyze

        Returns:
            List of tuples containing (sentiment, confidence_score)
        """
        try:
            self._ensure_initialized()

            # Check for empty list
            if not texts:
                return []

            # Filter out empty texts
            valid_texts = [text for text in texts if text and text.strip()]

            if not valid_texts:
                return [(SentimentEnum.NEUTRAL, 0.5) for _ in texts]

            # Perform prediction for the whole batch of texts
            results = self.sentiment_pipeline(valid_texts)

            # Process the results
            processed_results = []
            result_index = 0

            for text in texts:
                if not text or not text.strip():
                    # For empty texts, use neutral sentiment
                    processed_results.append((SentimentEnum.NEUTRAL, 0.5))
                else:
                    # For valid texts, use the model results
                    result = results[result_index]
                    result_index += 1

                    label = result['label']
                    score = result['score']
                    sentiment = self._map_to_sentiment_enum(label, score)

                    processed_results.append((sentiment, score))

            return processed_results

        except Exception as e:
            logger.error(f"Error during batch sentiment analysis: {e}")
            # Return list of neutral sentiments on error
            return [(SentimentEnum.NEUTRAL, 0.5) for _ in texts]
