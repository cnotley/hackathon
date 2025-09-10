import importlib
import json
import os
import sys
from datetime import datetime
from typing import Dict, Tuple

import pytest


class FakeS3Client:
    def __init__(self, objects: Dict[Tuple[str, str], Dict[str, object]]):
        self.objects = objects
        self.head_requests = []
        self.tag_requests = []

    def head_object(self, Bucket, Key):
        self.head_requests.append((Bucket, Key))
        try:
            obj = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise AssertionError(f"Unexpected head_object call for {(Bucket, Key)}") from error
        return {
            "ContentLength": obj["size"],
            "ContentType": obj.get("content_type", "application/pdf"),
            "Metadata": obj.get("metadata", {}),
            "ETag": obj.get("etag", "test-etag"),
            "LastModified": obj.get("last_modified", datetime.utcnow()),
        }

    def get_object_tagging(self, Bucket, Key):
        self.tag_requests.append((Bucket, Key))
        return {"TagSet": []}


class FakeStepFunctionsClient:
    def __init__(self):
        self.start_calls = []

    def start_execution(self, stateMachineArn, name, input):
        payload = json.loads(input)
        self.start_calls.append(
            {
                "stateMachineArn": stateMachineArn,
                "name": name,
                "input": payload,
            }
        )
        return {"executionArn": f"arn:aws:states:us-east-1:123456789012:execution/mock/{name}"}


@pytest.fixture
def load_ingestion(monkeypatch):
    def _loader(objects):
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        monkeypatch.setenv(
            "STATE_MACHINE_ARN",
            "arn:aws:states:us-east-1:123456789012:stateMachine:mock",
        )
        for module_name in [name for name in list(sys.modules) if name.startswith("lambda.ingestion_lambda")]:
            sys.modules.pop(module_name, None)
        module = importlib.import_module("lambda.ingestion_lambda")
        fake_s3 = FakeS3Client(objects)
        fake_sf = FakeStepFunctionsClient()
        module.s3_client = fake_s3
        module.stepfunctions_client = fake_sf
        return module, fake_s3, fake_sf

    return _loader


def test_pdf_event_starts_workflow(load_ingestion):
    module, _, fake_sf = load_ingestion(
        {
            ("labor-bucket", "invoice.pdf"): {
                "size": 2048,
                "etag": "labor",
                "metadata": {"source": "unit-test"},
                "last_modified": datetime(2024, 1, 1),
            }
        }
    )
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "labor-bucket"},
                    "object": {"key": "invoice.pdf"},
                }
            }
        ]
    }

    response = module.lambda_handler(event, None)

    assert response["statusCode"] == 200
    results = response["body"]["results"]
    assert results[0]["status"] == "workflow_started"
    assert fake_sf.start_calls, "Expected workflow to start for PDF input"

    start_payload = fake_sf.start_calls[0]["input"]
    assert start_payload["file_info"]["extension"] == ".pdf"
    assert start_payload["file_info"]["is_supported"] is True


def test_non_pdf_event_rejected(load_ingestion):
    module, _, fake_sf = load_ingestion({})
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "labor-bucket"},
                    "object": {"key": "invoice.xlsx"},
                }
            }
        ]
    }

    response = module.lambda_handler(event, None)

    assert response["statusCode"] == 400
    assert response["error"] == "Invalid file type"
    assert not fake_sf.start_calls


def test_multiple_pdf_files_load_individual_workflows(load_ingestion):
    module, fake_s3, fake_sf = load_ingestion(
        {
            ("labor", "one.pdf"): {"size": 1024, "etag": "one"},
            ("labor", "two.pdf"): {"size": 4096, "etag": "two"},
        }
    )

    event = {
        "Records": [
            {"s3": {"bucket": {"name": "labor"}, "object": {"key": "one.pdf"}}},
            {"s3": {"bucket": {"name": "labor"}, "object": {"key": "two.pdf"}}},
        ]
    }

    response = module.lambda_handler(event, None)

    assert response["statusCode"] == 200
    results = response["body"]["results"]
    assert len(results) == 2
    assert fake_s3.head_requests == [("labor", "one.pdf"), ("labor", "two.pdf")]
    assert len(fake_sf.start_calls) == 2


def test_pdf_payload_contains_labor_metadata(load_ingestion):
    module, _, fake_sf = load_ingestion(
        {
            ("labor", "labor_only.pdf"): {
                "size": 5120,
                "metadata": {"labor_only": "true", "project": "alpha"},
                "etag": "labor-only",
            }
        }
    )

    response = module.lambda_handler(
        {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "labor"},
                        "object": {"key": "labor_only.pdf"},
                    }
                }
            ]
        },
        None,
    )

    assert response["statusCode"] == 200
    payload = fake_sf.start_calls[0]["input"]
    metadata = payload["file_info"]["metadata"]
    assert metadata.get("labor_only") == "true"
    assert payload["file_info"]["content_type"] == "application/pdf"
    assert payload["bucket"] == "labor"


def test_pdf_rejects_large_file(load_ingestion):
    module, _, fake_sf = load_ingestion(
        {
            ("labor", "oversized.pdf"): {
                "size": 6 * 1024 * 1024,
                "etag": "oversized",
            }
        }
    )

    response = module.lambda_handler(
        {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "labor"},
                        "object": {"key": "oversized.pdf"},
                    }
                }
            ]
        },
        None,
    )

    assert response["statusCode"] == 400
    assert response["error"] == "Invalid file type"
    assert not fake_sf.start_calls
