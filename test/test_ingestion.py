import importlib
handle_event = importlib.import_module("lambda.ingestion_lambda").handle_event
validate_file_size = importlib.import_module("lambda.ingestion_lambda").validate_file_size


def test_handle_event_multi(monkeypatch):
    calls = []

    def fake_process(rec):
        calls.append(rec["s3"]["object"]["key"])
        return {"bucket": rec["s3"]["bucket"]["name"], "key": rec["s3"]["object"]["key"]}

    monkeypatch.setattr("lambda.ingestion_lambda.process_record", fake_process)
    evt = {
        "Records": [
            {"s3": {"bucket": {"name": "b"}, "object": {"key": "a.pdf"}}},
            {"s3": {"bucket": {"name": "b"}, "object": {"key": "b.pdf"}}},
        ]
    }
    res = handle_event(evt, None)
    assert len(calls) == 2
    assert len(res["batch"]) == 2


def test_seeding_trigger(monkeypatch):
    called = {}

    def fake_seed():
        called["ok"] = True

    monkeypatch.setattr("lambda.seeding.seed_rates", fake_seed)
    evt = {
        "Records": [
            {"s3": {"bucket": {"name": "b"}, "object": {"key": "rates.json", "size": 1024}}}
        ]
    }
    handle_event(evt, None)
    assert called.get("ok")

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


def test_head_object_failure(monkeypatch):
    class FakeS3Client:
        def head_object(self, Bucket, Key):
            raise Exception("boom")

    fake_s3 = FakeS3Client()

    def fake_client(service):
        assert service == "s3"
        return fake_s3

    monkeypatch.setattr("lambda.ingestion_lambda.client", fake_client)

    evt = {"Records": [{"s3": {"bucket": {"name": "invoices"}, "object": {"key": "missing.pdf"}}}]}
    res = handle_event(evt, None)
    assert res["batch"][0]["error"] == "unknown_size"

def test_validate_file_size():
    validate_file_size(1024)  # under limit
    import pytest
    with pytest.raises(ValueError):
        validate_file_size(6 * 1024 * 1024)
