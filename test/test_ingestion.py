import importlib
handle_event = importlib.import_module("lambda.ingestion_lambda").handle_event
validate_file_size = importlib.import_module("lambda.ingestion_lambda").validate_file_size

def test_small_file_ok():
    evt = {"Records":[{"s3":{"bucket":{"name":"invoices"},"object":{"key":"test_invoice.pdf","size":1024}}}]}
    res = handle_event(evt, None)
    assert res["batch"][0]["bucket"] == "invoices"

def test_large_file_block():
    evt = {"Records":[{"s3":{"bucket":{"name":"invoices"},"object":{"key":"big.pdf","size":6*1024*1024}}}]}
    res = handle_event(evt, None)
    assert "error" in res["batch"][0]


def test_batch_mixed_sizes():
    evt = {
        "Records": [
            {"s3": {"bucket": {"name": "invoices"}, "object": {"key": "ok.pdf", "size": 1024}}},
            {"s3": {"bucket": {"name": "invoices"}, "object": {"key": "big.pdf", "size": 7 * 1024 * 1024}}},
        ]
    }
    res = handle_event(evt, None)
    assert len(res["batch"]) == 2
    assert "error" in res["batch"][1]


def test_validate_file_size():
    validate_file_size(1024)  # under limit
    import pytest
    with pytest.raises(ValueError):
        validate_file_size(6 * 1024 * 1024)
