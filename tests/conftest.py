from unittest.mock import Mock

import pytest


@pytest.fixture(autouse=True)
def kafka(monkeypatch) -> None:
    kafka = Mock()
    mock = Mock(return_value=kafka)
    monkeypatch.setattr('confluent_kafka.Consumer', mock)

    yield kafka
