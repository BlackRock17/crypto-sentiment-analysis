"""
Module for sentiment analysis of tweets using a pre-trained model.
"""
import logging
import numpy as np
from typing import Dict, Tuple, Optional, List
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig
from scipy.special import softmax

from src.data_processing.models.database import SentimentEnum

# Set up logging
logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """
    Class for analyzing sentiment in text using a pre-trained Twitter RoBERTa model.
    """

    def __init__(self, model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest"):
        """
        Initialize the sentiment analyzer.

        Args:
            model_name: Name of the pre-trained model to use
        """
        self.model_name = model_name
        self.model = None
        self.tokenizer = None
        self.config = None
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
        Initialize the sentiment model and tokenizer.
        """
        logger.info(f"Loading sentiment analysis model: {self.model_name}")
        try:
            # Load pre-trained model for sentiment classification
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.config = AutoConfig.from_pretrained(self.model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)

            # Move model to correct device
            self.model.to(self.device)
            logger.info(f"Device set to use {self.device}")

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

    def _preprocess_text(self, text):
        """
        Preprocess text to handle Twitter-specific elements.

        Args:
            text: The raw text

        Returns:
            Preprocessed text
        """
        new_text = []
        for t in text.split(" "):
            t = '@user' if t.startswith('@') and len(t) > 1 else t
            t = 'http' if t.startswith('http') else t
            new_text.append(t)
        return " ".join(new_text)

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

            # Preprocess text
            preprocessed_text = self._preprocess_text(text)

            # Tokenize and get model prediction
            encoded_input = self.tokenizer(preprocessed_text, return_tensors='pt').to(self.device)
            output = self.model(**encoded_input)
            scores = output[0][0].detach().cpu().numpy()
            scores = softmax(scores)

            # Get the highest scoring sentiment
            ranking = np.argsort(scores)
            ranking = ranking[::-1]

            top_label_id = ranking[0]
            top_label = self.config.id2label[top_label_id]
            confidence = scores[top_label_id]

            # Map the label to our SentimentEnum
            sentiment = self._map_to_sentiment_enum(top_label)

            # Log truncated text to avoid cluttering logs
            truncated_text = text[:50] + ('...' if len(text) > 50 else '')
            logger.info(f"Analyzed text: '{truncated_text}' -> {sentiment.value} ({confidence:.4f})")

            return sentiment, float(confidence)

        except Exception as e:
            logger.error(f"Error during sentiment analysis: {e}")
            # Return neutral sentiment with low confidence on error
            return SentimentEnum.NEUTRAL, 0.5

    def _map_to_sentiment_enum(self, label: str) -> SentimentEnum:
        """
        Map the model output label to our SentimentEnum.

        Args:
            label: The label from the model (Positive, Negative, Neutral)

        Returns:
            Corresponding SentimentEnum value
        """
        label_lower = label.lower()
        if label_lower == 'positive':
            return SentimentEnum.POSITIVE
        elif label_lower == 'negative':
            return SentimentEnum.NEGATIVE
        else:  # 'neutral'
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
            results = []

            if not valid_texts:
                return [(SentimentEnum.NEUTRAL, 0.5) for _ in texts]

            # Process each text individually
            # We could batch this, but for simplicity we'll process one at a time
            for text in texts:
                if not text or not text.strip():
                    # For empty texts, use neutral sentiment
                    results.append((SentimentEnum.NEUTRAL, 0.5))
                else:
                    # For valid texts, analyze sentiment
                    sentiment, confidence = self.analyze_text(text)
                    results.append((sentiment, confidence))

            return results

        except Exception as e:
            logger.error(f"Error during batch sentiment analysis: {e}")
            # Return list of neutral sentiments on error
            return [(SentimentEnum.NEUTRAL, 0.5) for _ in texts]
