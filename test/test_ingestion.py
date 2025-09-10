from lambda.ingestion_lambda import handle_event

def test_small_file_ok():
    evt = {"Records":[{"s3":{"bucket":{"name":"invoices"},"object":{"key":"test_invoice.pdf","size":1024}}}]}
    res = handle_event(evt, None)
    assert res["batch"][0]["bucket"] == "invoices"

def test_large_file_block():
    evt = {"Records":[{"s3":{"bucket":{"name":"invoices"},"object":{"key":"big.pdf","size":6*1024*1024}}}]}
    res = handle_event(evt, None)
    assert "error" in res["batch"][0]
