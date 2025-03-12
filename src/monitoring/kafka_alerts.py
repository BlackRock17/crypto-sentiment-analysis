import logging
import time
import smtplib
import threading
from typing import Dict, List, Any, Optional, Callable
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configure logger
logger = logging.getLogger(__name__)


class KafkaAlertManager:
    """
    Alert manager for Kafka events.
    Handles alerting for critical issues with Kafka components.
    """

    # Alert levels
    LEVEL_INFO = "info"
    LEVEL_WARNING = "warning"
    LEVEL_ERROR = "error"
    LEVEL_CRITICAL = "critical"

    def __init__(self, email_config: Optional[Dict[str, str]] = None):
        """
        Initialize the alert manager.

        Args:
            email_config: Optional email configuration for sending alerts
                {
                    "smtp_server": "smtp.example.com",
                    "smtp_port": "587",
                    "username": "alerts@example.com",
                    "password": "password",
                    "from_address": "alerts@example.com",
                    "to_addresses": ["admin1@example.com", "admin2@example.com"]
                }
        """
        self.email_config = email_config
        self.alert_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self.alert_history: List[Dict[str, Any]] = []
        self.max_history = 100  # Maximum number of alerts to keep in history
        self.alert_deduplication_window = 300  # 5 minutes in seconds
        self.recent_alerts = {}  # For deduplication

    def add_alert_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Add a callback function to be called when an alert is triggered.

        Args:
            callback: Function that takes an alert dictionary
        """
        self.alert_callbacks.append(callback)

    def trigger_alert(self,
                      alert_type: str,
                      level: str,
                      message: str,
                      component: str = "kafka",
                      details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Trigger an alert with the specified information.

        Args:
            alert_type: Type of alert (broker_down, consumer_lag, etc.)
            level: Alert level (info, warning, error, critical)
            message: Human-readable alert message
            component: Component that triggered the alert
            details: Additional details about the alert

        Returns:
            Alert dictionary
        """
        # Create alert data
        alert = {
            "timestamp": time.time(),
            "type": alert_type,
            "level": level,
            "message": message,
            "component": component,
            "details": details or {}
        }

        # Check for duplicate alerts
        alert_key = f"{alert_type}:{component}"
        current_time = time.time()

        if alert_key in self.recent_alerts:
            last_time, last_level = self.recent_alerts[alert_key]

            # If a similar alert was triggered recently and this isn't higher severity, don't trigger
            if (current_time - last_time < self.alert_deduplication_window and
                    not self._is_higher_level(level, last_level)):
                logger.debug(f"Suppressing duplicate alert: {alert_type}")
                return alert

        # Update recent alerts
        self.recent_alerts[alert_key] = (current_time, level)

        # Add to history
        self.alert_history.append(alert)

        # Trim history if needed
        if len(self.alert_history) > self.max_history:
            self.alert_history = self.alert_history[-self.max_history:]

        # Log the alert
        log_method = getattr(logger, level if level in ('info', 'warning', 'error', 'critical') else 'warning')
        log_method(f"Kafka Alert: [{level.upper()}] {message}")

        # Call alert callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Error in alert callback: {e}")

        # Send email for higher severity alerts
        if level in (self.LEVEL_ERROR, self.LEVEL_CRITICAL) and self.email_config:
            threading.Thread(target=self._send_alert_email, args=(alert,)).start()

        return alert

    def _is_higher_level(self, level1: str, level2: str) -> bool:
        """
        Check if level1 is higher severity than level2.

        Args:
            level1: First alert level
            level2: Second alert level

        Returns:
            True if level1 is higher severity than level2
        """
        levels = {
            self.LEVEL_INFO: 0,
            self.LEVEL_WARNING: 1,
            self.LEVEL_ERROR: 2,
            self.LEVEL_CRITICAL: 3
        }

        return levels.get(level1, 0) > levels.get(level2, 0)

    def _send_alert_email(self, alert: Dict[str, Any]):
        """
        Send an email alert.

        Args:
            alert: Alert dictionary
        """
        if not self.email_config:
            return

        try:
            # Create message
            msg = MIMEMultipart()
            msg["Subject"] = f"[{alert['level'].upper()}] Kafka Alert: {alert['type']}"
            msg["From"] = self.email_config["from_address"]
            msg["To"] = ", ".join(self.email_config["to_addresses"])

            # Create message body
            body = f"""
            Kafka Alert:

            Type: {alert['type']}
            Level: {alert['level'].upper()}
            Component: {alert['component']}
            Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(alert['timestamp']))}

            Message:
            {alert['message']}

            Details:
            {alert.get('details', {})}
            """

            msg.attach(MIMEText(body, "plain"))

            # Send email
            with smtplib.SMTP(self.email_config["smtp_server"], int(self.email_config["smtp_port"])) as server:
                server.starttls()
                server.login(self.email_config["username"], self.email_config["password"])
                server.send_message(msg)

            logger.info(f"Sent alert email for {alert['type']}")

        except Exception as e:
            logger.error(f"Error sending alert email: {e}")
