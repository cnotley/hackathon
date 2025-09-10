import importlib
from unittest.mock import Mock
import pytest

textract_module = importlib.import_module("lambda.extraction_lambda")


def test_textract_analyze_timeout(monkeypatch):
    # Mock Textract client to always return IN_PROGRESS
    dummy = Mock()
    dummy.start_document_analysis.return_value = {"JobId": "123"}
    dummy.get_document_analysis.return_value = {"JobStatus": "IN_PROGRESS"}

    monkeypatch.setattr(textract_module, "client", lambda service: dummy)
    # Avoid sleeping during test
    monkeypatch.setattr(textract_module.time, "sleep", lambda x: None)

    with pytest.raises(TimeoutError) as excinfo:
        textract_module._textract_analyze("bucket", "file.pdf", max_attempts=2)

    assert "timed out" in str(excinfo.value).lower()
    # Ensure polling happened expected number of times
    assert dummy.get_document_analysis.call_count == 2
