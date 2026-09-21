import runpy
from unittest.mock import patch

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import load_data, run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 1000,
        "cpu_percent": 95,
        "memory_percent": 95,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1
    assert messages[0]["type"] == "ANOMALY"
    assert messages[0]["service"] == "payment-service"


def test_load_data(tmp_path):
    data_file = tmp_path / "data.json"

    data_file.write_text(
        '[{"service": "payment-service", "response_time_ms": 100}]',
        encoding="utf-8"
    )

    data = load_data(data_file)

    assert len(data) == 1
    assert data[0]["service"] == "payment-service"


def test_run_pipeline(tmp_path):
    data_file = tmp_path / "data.json"

    data_file.write_text(
        """[
            {
                "timestamp": "2026-09-20T10:00:00",
                "service": "payment-service",
                "response_time_ms": 120,
                "cpu_percent": 42,
                "memory_percent": 51,
                "log_level": "INFO",
                "message": "Payment request processed successfully"
            },
            {
                "timestamp": "2026-09-20T10:05:00",
                "service": "payment-service",
                "response_time_ms": 1000,
                "cpu_percent": 95,
                "memory_percent": 95,
                "log_level": "ERROR",
                "message": "Payment service timeout"
            }
        ]""",
        encoding="utf-8"
    )

    result = run_pipeline(data_file)

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert len(result["events_consumed"]) == 0


def test_main_block(capsys):
    events = [
        {
            "timestamp": "2026-09-20T10:05:00",
            "service": "payment-service",
            "type": "ANOMALY",
            "reasons": ["High response time"]
        }
    ]

    from event_consumer import EventConsumer as PipelineEventConsumer

    with patch.object(
        PipelineEventConsumer,
        "consume",
        return_value=events
    ):
        runpy.run_module("src.aiops_pipeline", run_name="__main__")

    output = capsys.readouterr().out

    assert "AIOps Pipeline Result" in output
    assert "Records processed:" in output
    assert "Anomalies detected:" in output
    assert "Events consumed:" in output
    assert "Service: payment-service" in output
    assert "Timestamp: 2026-09-20T10:05:00" in output
    assert "Type: ANOMALY" in output
    assert "High response time" in output