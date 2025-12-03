import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
import requests
import json
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """Alert definition."""

    severity: str  # 'critical', 'warning', 'info'
    title: str
    message: str
    metric: str
    value: float
    threshold: float
    timestamp: datetime


class AlertManager:
    """Manage alerts and notifications."""

    def __init__(self, config: Dict):
        self.config = config
        self.alert_history: List[Alert] = []

    async def check_metrics(self, current_metrics: Dict) -> List[Alert]:
        """Check current metrics against thresholds."""
        alerts = []

        # Latency alerts
        if current_metrics.get("api_latency_p95", 0) > self.config.get(
            "latency_threshold", 1.0
        ):
            alerts.append(
                Alert(
                    severity="critical",
                    title="High API Latency",
                    message=f'P95 API latency is {current_metrics["api_latency_p95"]:.2f}s',
                    metric="api_latency_p95",
                    value=current_metrics["api_latency_p95"],
                    threshold=self.config["latency_threshold"],
                    timestamp=datetime.utcnow(),
                )
            )

        # Error rate alerts
        error_rate = current_metrics.get("error_rate", 0)
        if error_rate > self.config.get("error_rate_threshold", 0.05):
            alerts.append(
                Alert(
                    severity="critical" if error_rate > 0.1 else "warning",
                    title="High Error Rate",
                    message=f"Current error rate is {error_rate:.1%}",
                    metric="error_rate",
                    value=error_rate,
                    threshold=self.config["error_rate_threshold"],
                    timestamp=datetime.utcnow(),
                )
            )

        # Feature computation delay alerts
        delay = current_metrics.get("feature_computation_delay", 0)
        if delay > self.config.get("computation_delay_threshold", 10.0):
            alerts.append(
                Alert(
                    severity="warning",
                    title="Feature Computation Delay",
                    message=f"Feature computation delay is {delay:.2f}s",
                    metric="feature_computation_delay",
                    value=delay,
                    threshold=self.config["computation_delay_threshold"],
                    timestamp=datetime.utcnow(),
                )
            )

        # Store alerts and send notifications
        for alert in alerts:
            await self._handle_alert(alert)

        return alerts

    async def _handle_alert(self, alert: Alert):
        """Handle alert notification."""
        self.alert_history.append(alert)

        # Keep only last 1000 alerts
        if len(self.alert_history) > 1000:
            self.alert_history = self.alert_history[-1000:]

        # Check if similar alert was recently sent (deduplication)
        if self._is_duplicate_alert(alert):
            logger.info(f"Skipping duplicate alert: {alert.title}")
            return

        # Send notifications based on severity
        if alert.severity == "critical":
            await self._send_critical_alert(alert)
        elif alert.severity == "warning":
            await self._send_warning_alert(alert)

        logger.info(f"Alert triggered: {alert.title} - {alert.message}")

    def _is_duplicate_alert(self, alert: Alert) -> bool:
        """Check if similar alert was sent recently."""
        recent_alerts = [
            a
            for a in self.alert_history[-10:]  # Check last 10 alerts
            if a.metric == alert.metric and a.severity == alert.severity
        ]

        if not recent_alerts:
            return False

        # Consider duplicate if same metric alert in last 5 minutes
        time_threshold = datetime.utcnow() - timedelta(minutes=5)
        recent_duplicates = [a for a in recent_alerts if a.timestamp > time_threshold]

        return len(recent_duplicates) > 0

    async def _send_critical_alert(self, alert: Alert):
        """Send critical alert notifications."""
        tasks = []

        if self.config.get("slack_webhook"):
            tasks.append(self._send_slack_alert(alert, self.config["slack_webhook"]))

        if self.config.get("email_config"):
            tasks.append(self._send_email_alert(alert, self.config["email_config"]))

        if self.config.get("pagerduty_integration_key"):
            tasks.append(
                self._send_pagerduty_alert(
                    alert, self.config["pagerduty_integration_key"]
                )
            )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_warning_alert(self, alert: Alert):
        """Send warning alert notifications."""
        if self.config.get("slack_webhook"):
            await self._send_slack_alert(alert, self.config["slack_webhook"])

    async def _send_slack_alert(self, alert: Alert, webhook_url: str):
        """Send alert to Slack."""
        try:
            message = {
                "text": f"🚨 *{alert.title}*",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"🚨 {alert.title}"},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Metric:* {alert.metric}"},
                            {"type": "mrkdwn", "text": f"*Value:* {alert.value:.2f}"},
                            {
                                "type": "mrkdwn",
                                "text": f"*Threshold:* {alert.threshold:.2f}",
                            },
                            {"type": "mrkdwn", "text": f"*Severity:* {alert.severity}"},
                        ],
                    },
                    {
                        "type": "section",
                        "text": {"type": "plain_text", "text": alert.message},
                    },
                ],
            }

            response = requests.post(webhook_url, json=message, timeout=10)
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")

    async def _send_email_alert(self, alert: Alert, email_config: Dict):
        """Send alert via email."""
        try:
            message = MimeMultipart()
            message["From"] = email_config["from_email"]
            message["To"] = ", ".join(email_config["to_emails"])
            message["Subject"] = f"[{alert.severity.upper()}] {alert.title}"

            body = f"""
            Alert Details:
            - Title: {alert.title}
            - Message: {alert.message}
            - Metric: {alert.metric}
            - Current Value: {alert.value:.2f}
            - Threshold: {alert.threshold:.2f}
            - Severity: {alert.severity}
            - Timestamp: {alert.timestamp}
            
            Please investigate immediately if critical.
            """

            message.attach(MimeText(body, "plain"))

            with smtplib.SMTP(
                email_config["smtp_server"], email_config["smtp_port"]
            ) as server:
                server.starttls()
                server.login(
                    email_config["smtp_username"], email_config["smtp_password"]
                )
                server.send_message(message)

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")

    async def _send_pagerduty_alert(self, alert: Alert, integration_key: str):
        """Send alert to PagerDuty."""
        try:
            payload = {
                "routing_key": integration_key,
                "event_action": "trigger",
                "payload": {
                    "summary": alert.title,
                    "source": "voice-feature-store",
                    "severity": alert.severity,
                    "custom_details": {
                        "message": alert.message,
                        "metric": alert.metric,
                        "value": alert.value,
                        "threshold": alert.threshold,
                    },
                },
            }

            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue", json=payload, timeout=10
            )
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to send PagerDuty alert: {e}")

    def get_alert_summary(self, hours: int = 24) -> Dict:
        """Get alert summary for the last N hours."""
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        recent_alerts = [a for a in self.alert_history if a.timestamp > time_threshold]

        critical_alerts = [a for a in recent_alerts if a.severity == "critical"]
        warning_alerts = [a for a in recent_alerts if a.severity == "warning"]

        return {
            "total_alerts": len(recent_alerts),
            "critical_alerts": len(critical_alerts),
            "warning_alerts": len(warning_alerts),
            "alerts_by_metric": self._group_alerts_by_metric(recent_alerts),
            "recent_alerts": [
                {
                    "title": a.title,
                    "severity": a.severity,
                    "timestamp": a.timestamp.isoformat(),
                    "metric": a.metric,
                }
                for a in recent_alerts[-10:]  # Last 10 alerts
            ],
        }

    def _group_alerts_by_metric(self, alerts: List[Alert]) -> Dict:
        """Group alerts by metric."""
        grouped = {}
        for alert in alerts:
            if alert.metric not in grouped:
                grouped[alert.metric] = 0
            grouped[alert.metric] += 1
        return grouped
