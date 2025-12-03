#!/usr/bin/env python3
"""
Operational Dashboard for Voice Feature Store
Provides real-time insights into system health, performance, and business metrics.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd
import numpy as np
from prometheus_api_client import PrometheusConnect
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
import click

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

console = Console()


class OperationalDashboard:
    """Real-time operational dashboard for Voice Feature Store."""

    def __init__(self, prometheus_url: str, api_url: str):
        self.prometheus = PrometheusConnect(url=prometheus_url, disable_ssl=True)
        self.api_url = api_url
        self.metrics_cache = {}

    async def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health status."""
        try:
            health_metrics = {}

            # API health
            health_metrics["api"] = await self._check_api_health()

            # Redis health
            health_metrics["redis"] = await self._check_redis_health()

            # Kafka health
            health_metrics["kafka"] = await self._check_kafka_health()

            # Feature computation health
            health_metrics["feature_computation"] = (
                await self._check_feature_computation_health()
            )

            # Overall status
            health_metrics["overall"] = all(
                [
                    health_metrics["api"]["status"] == "healthy",
                    health_metrics["redis"]["status"] == "healthy",
                    health_metrics["kafka"]["status"] == "healthy",
                    health_metrics["feature_computation"]["status"] == "healthy",
                ]
            )

            return health_metrics

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"overall": False, "error": str(e)}

    async def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics."""
        try:
            # Query Prometheus for key metrics
            queries = {
                "api_latency": "histogram_quantile(0.95, rate(api_request_latency_seconds_bucket[5m]))",
                "error_rate": 'rate(api_requests_total{status=~"5.."}[5m]) / rate(api_requests_total[5m])',
                "feature_computation_rate": "rate(feature_computation_total[5m])",
                "prediction_latency": "histogram_quantile(0.95, rate(model_prediction_latency_seconds_bucket[5m]))",
                "memory_usage": 'container_memory_usage_bytes{container="api"}',
                "cpu_usage": 'rate(container_cpu_usage_seconds_total{container="api"}[5m])',
            }

            metrics = {}
            for name, query in queries.items():
                try:
                    result = self.prometheus.custom_query(query)
                    if result:
                        metrics[name] = float(result[0]["value"][1])
                except Exception as e:
                    logger.warning(f"Failed to query {name}: {e}")
                    metrics[name] = 0.0

            return metrics

        except Exception as e:
            logger.error(f"Performance metrics failed: {e}")
            return {}

    async def get_business_metrics(self) -> Dict[str, Any]:
        """Get business-level metrics."""
        try:
            business_metrics = {}

            # Active calls
            business_metrics["active_calls"] = await self._get_active_calls_count()

            # Features computed today
            business_metrics["features_today"] = (
                await self._get_features_computed_today()
            )

            # Predictions served
            business_metrics["predictions_served"] = (
                await self._get_predictions_served()
            )

            # Data quality score
            business_metrics["data_quality"] = await self._get_data_quality_score()

            return business_metrics

        except Exception as e:
            logger.error(f"Business metrics failed: {e}")
            return {}

    async def get_alerts_status(self) -> List[Dict[str, Any]]:
        """Get current alert status."""
        try:
            # Query Prometheus alerts
            alerts = self.prometheus.get_current_alerts()

            active_alerts = []
            for alert in alerts:
                if alert["state"] == "firing":
                    active_alerts.append(
                        {
                            "name": alert["labels"]["alertname"],
                            "severity": alert["labels"].get("severity", "warning"),
                            "description": alert["annotations"].get("description", ""),
                            "since": alert["activeAt"],
                        }
                    )

            return active_alerts

        except Exception as e:
            logger.error(f"Alerts status failed: {e}")
            return []

    async def _check_api_health(self) -> Dict[str, Any]:
        """Check API health."""
        try:
            import requests

            response = requests.get(f"{self.api_url}/health", timeout=5)
            return {
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "response_time": response.elapsed.total_seconds(),
                "status_code": response.status_code,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def _check_redis_health(self) -> Dict[str, Any]:
        """Check Redis health via metrics."""
        try:
            # Query Redis metrics from Prometheus
            query = "redis_up"
            result = self.prometheus.custom_query(query)
            if result and float(result[0]["value"][1]) == 1:
                return {"status": "healthy", "connected": True}
            else:
                return {"status": "unhealthy", "connected": False}
        except Exception as e:
            return {"status": "unknown", "error": str(e)}

    async def _check_kafka_health(self) -> Dict[str, Any]:
        """Check Kafka health via consumer lag."""
        try:
            # Query Kafka consumer lag
            query = "kafka_consumer_lag"
            result = self.prometheus.custom_query(query)
            if result:
                max_lag = max([float(r["value"][1]) for r in result])
                status = "healthy" if max_lag < 1000 else "degraded"
                return {"status": status, "max_lag": max_lag}
            else:
                return {"status": "unknown", "max_lag": 0}
        except Exception as e:
            return {"status": "unknown", "error": str(e)}

    async def _check_feature_computation_health(self) -> Dict[str, Any]:
        """Check feature computation health."""
        try:
            # Query feature computation metrics
            query = "rate(feature_computation_total[5m])"
            result = self.prometheus.custom_query(query)
            if result and float(result[0]["value"][1]) > 0:
                return {"status": "healthy", "rate": float(result[0]["value"][1])}
            else:
                return {"status": "unhealthy", "rate": 0}
        except Exception as e:
            return {"status": "unknown", "error": str(e)}

    async def _get_active_calls_count(self) -> int:
        """Get count of active calls."""
        try:
            # This would query your call tracking system
            # For now, return a mock value
            return 1500
        except Exception:
            return 0

    async def _get_features_computed_today(self) -> int:
        """Get features computed today."""
        try:
            query = "sum(increase(feature_computation_total[24h]))"
            result = self.prometheus.custom_query(query)
            if result:
                return int(float(result[0]["value"][1]))
            return 0
        except Exception:
            return 0

    async def _get_predictions_served(self) -> int:
        """Get predictions served today."""
        try:
            query = "sum(increase(model_predictions_total[24h]))"
            result = self.prometheus.custom_query(query)
            if result:
                return int(float(result[0]["value"][1]))
            return 0
        except Exception:
            return 0

    async def _get_data_quality_score(self) -> float:
        """Get data quality score."""
        try:
            # This would compute based on various quality metrics
            # For now, return a mock value
            return 0.95
        except Exception:
            return 0.0


def create_dashboard_layout(
    health: Dict, performance: Dict, business: Dict, alerts: List[Dict]
) -> Layout:
    """Create the dashboard layout."""
    layout = Layout()

    # Split into main sections
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main"),
        Layout(name="footer", size=3),
    )

    # Split main section
    layout["main"].split_row(Layout(name="left"), Layout(name="right"))

    # Split left section
    layout["left"].split_column(Layout(name="health"), Layout(name="performance"))

    # Split right section
    layout["right"].split_column(Layout(name="business"), Layout(name="alerts"))

    # Populate sections
    layout["header"].update(create_header_panel())
    layout["health"].update(create_health_panel(health))
    layout["performance"].update(create_performance_panel(performance))
    layout["business"].update(create_business_panel(business))
    layout["alerts"].update(create_alerts_panel(alerts))
    layout["footer"].update(create_footer_panel())

    return layout


def create_header_panel() -> Panel:
    """Create header panel."""
    title = Text("🎤 Voice Feature Store - Operational Dashboard", style="bold blue")
    subtitle = Text("Real-time System Monitoring & Analytics", style="dim")

    content = Text()
    content.append(title)
    content.append("\n")
    content.append(subtitle)

    return Panel(content, style="round")


def create_health_panel(health: Dict) -> Panel:
    """Create health status panel."""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Component", style="dim")
    table.add_column("Status")
    table.add_column("Details")

    for component, status_info in health.items():
        if component == "overall":
            continue

        status = status_info.get("status", "unknown")
        status_style = (
            "green"
            if status == "healthy"
            else "red" if status == "unhealthy" else "yellow"
        )

        details = []
        for key, value in status_info.items():
            if key != "status":
                details.append(f"{key}: {value}")

        table.add_row(
            component.upper(),
            Text(status.upper(), style=status_style),
            ", ".join(details),
        )

    overall_status = "HEALTHY" if health.get("overall") else "UNHEALTHY"
    overall_style = "green" if health.get("overall") else "red"

    return Panel(
        table,
        title=f"System Health - [bold {overall_style}]{overall_status}[/]",
        border_style=overall_style,
    )


def create_performance_panel(performance: Dict) -> Panel:
    """Create performance metrics panel."""
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="dim")
    table.add_column("Value")
    table.add_column("Status")

    metrics_config = {
        "api_latency": ("API Latency (P95)", "ms", 1.0, 0.5),
        "error_rate": ("Error Rate", "%", 5.0, 1.0),
        "feature_computation_rate": ("Features/sec", "/s", 1000, 100),
        "prediction_latency": ("Prediction Latency", "ms", 100, 50),
    }

    for metric_key, (
        name,
        unit,
        warn_threshold,
        good_threshold,
    ) in metrics_config.items():
        value = performance.get(metric_key, 0)

        if metric_key == "error_rate":
            value *= 100  # Convert to percentage

        status_style = (
            "red"
            if value > warn_threshold
            else "green" if value <= good_threshold else "yellow"
        )
        status_text = (
            "✓" if value <= good_threshold else "⚠" if value <= warn_threshold else "✗"
        )

        table.add_row(name, f"{value:.2f}{unit}", Text(status_text, style=status_style))

    return Panel(table, title="Performance Metrics", border_style="cyan")


def create_business_panel(business: Dict) -> Panel:
    """Create business metrics panel."""
    table = Table(show_header=True, header_style="bold green")
    table.add_column("Metric", style="dim")
    table.add_column("Value")
    table.add_column("Trend")

    metrics = [
        ("Active Calls", business.get("active_calls", 0), "📞"),
        ("Features Today", business.get("features_today", 0), "⚡"),
        ("Predictions Served", business.get("predictions_served", 0), "🤖"),
        ("Data Quality", f"{business.get('data_quality', 0)*100:.1f}%", "📊"),
    ]

    for name, value, emoji in metrics:
        table.add_row(name, str(value), emoji)

    return Panel(table, title="Business Metrics", border_style="green")


def create_alerts_panel(alerts: List[Dict]) -> Panel:
    """Create alerts panel."""
    if not alerts:
        return Panel(
            Text("No active alerts 🎉", style="green", justify="center"),
            title="Active Alerts",
            border_style="green",
        )

    table = Table(show_header=True, header_style="bold red")
    table.add_column("Severity", style="dim")
    table.add_column("Alert")
    table.add_column("Since")

    for alert in alerts:
        severity_style = "red" if alert["severity"] == "critical" else "yellow"
        table.add_row(
            Text(alert["severity"].upper(), style=severity_style),
            alert["name"],
            alert["since"],
        )

    return Panel(table, title=f"Active Alerts ({len(alerts)})", border_style="red")


def create_footer_panel() -> Panel:
    """Create footer panel."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    footer_text = Text(f"Last updated: {timestamp} | Press Ctrl+C to exit", style="dim")
    return Panel(footer_text, style="dim")


@click.command()
@click.option(
    "--prometheus-url", default="http://localhost:9090", help="Prometheus URL"
)
@click.option(
    "--api-url", default="http://localhost:8000", help="Feature Store API URL"
)
@click.option("--refresh-interval", default=5, help="Refresh interval in seconds")
def main(prometheus_url: str, api_url: str, refresh_interval: int):
    """Run the operational dashboard."""
    dashboard = OperationalDashboard(prometheus_url, api_url)

    async def update_dashboard():
        """Update dashboard data."""
        health = await dashboard.get_system_health()
        performance = await dashboard.get_performance_metrics()
        business = await dashboard.get_business_metrics()
        alerts = await dashboard.get_alerts_status()

        return create_dashboard_layout(health, performance, business, alerts)

    async def run_dashboard():
        """Run the dashboard with live updates."""
        with Live(auto_refresh=False) as live:
            while True:
                try:
                    layout = await update_dashboard()
                    live.update(layout, refresh=True)
                    await asyncio.sleep(refresh_interval)
                except KeyboardInterrupt:
                    console.print("\n[yellow]Dashboard stopped by user[/yellow]")
                    break
                except Exception as e:
                    console.print(f"\n[red]Dashboard error: {e}[/red]")
                    await asyncio.sleep(refresh_interval)

    console.print("🚀 Starting Voice Feature Store Operational Dashboard...")
    asyncio.run(run_dashboard())


if __name__ == "__main__":
    main()
