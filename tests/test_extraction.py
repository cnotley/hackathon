import importlib
import os
import sys
from typing import Any, Dict

import pytest


@pytest.fixture
def load_extraction(monkeypatch):
    def _loader():
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        for module_name in list(sys.modules):
            if module_name.startswith("lambda.extraction_lambda"):
                sys.modules.pop(module_name)
        module = importlib.import_module("lambda.extraction_lambda")
        return module

    return _loader


def _build_event(key: str, size: int = 1024, extension: str = ".pdf") -> Dict[str, Any]:
    return {
        "task": "extract",
        "input": {
            "file_info": {"key": key, "size": size, "extension": extension},
            "bucket": "labor-bucket",
        },
    }


def test_lambda_handler_rejects_non_pdf(load_extraction):
    module = load_extraction()
    event = _build_event("invoice.xlsx", extension=".xlsx")

    with pytest.raises(ValueError, match="Unsupported file type"):
        module.lambda_handler(event, None)


def test_labor_only_filter_removes_material_tables(load_extraction, monkeypatch):
    module = load_extraction()

    fake_basic = {
        "text_blocks": [],
        "tables": [
            {
                "page": 1,
                "table_id": "labor-table",
                "rows": [[{"text": "Labor"}, {"text": "$100"}]],
            },
            {
                "page": 2,
                "table_id": "materials-table",
                "rows": [[{"text": "Materials"}, {"text": "$200"}]],
            },
        ],
        "sheets": [
            {"sheet_name": "Labor Summary", "columns": ["Item"], "data": [{"Item": "Labor"}]},
            {"sheet_name": "Materials", "columns": ["Item"], "data": [{"Item": "Materials"}]},
        ],
        "summary": {"sheet_names": [], "total_sheets": 0},
        "processing_metadata": {"is_async": False},
    }

    fake_chunks = [
        {"type": "text", "content": "Labor", "metadata": {}, "chunk_metadata": {"chunk_id": "1"}},
        {"type": "text", "content": "Labor continued", "metadata": {}, "chunk_metadata": {"chunk_id": "2"}},
    ]

    fake_result = {
        "extraction_status": "completed",
        "extracted_data": fake_basic,
        "semantic_chunks": fake_chunks,
        "processing_summary": {"processing_method": "sync", "total_chunks": 2},
        "normalized_data": {"labor": [{"name": "Worker", "total": 100}]},
    }

    monkeypatch.setattr(
        module,
        "IntelligentExtractor",
        lambda: type("_", (), {"process_document_intelligently": lambda *_: fake_result}),
    )

    result = module.lambda_handler(_build_event("invoice.pdf"), None)

    assert result["extraction_status"] == "completed"
    tables = result["extracted_data"]["tables"]
    assert [table["table_id"] for table in tables] == ["labor-table"]
    sheets = result["extracted_data"]["sheets"]
    assert [sheet["sheet_name"] for sheet in sheets] == ["Labor Summary"]
    assert result["normalized_data"]["labor"]


def test_chunk_overlap_includes_contiguous_labor(load_extraction, monkeypatch):
    module = load_extraction()

    fake_result = {
        "extraction_status": "completed",
        "extracted_data": {"text_blocks": []},
        "semantic_chunks": [
            {
                "type": "text",
                "content": "Labor hours 0-8",
                "metadata": {},
                "chunk_metadata": {"chunk_id": "1", "start_index": 0, "end_index": 8},
            },
            {
                "type": "text",
                "content": "Labor hours 6-14",
                "metadata": {},
                "chunk_metadata": {"chunk_id": "2", "start_index": 6, "end_index": 14},
            },
        ],
        "processing_summary": {"processing_method": "sync", "total_chunks": 2},
        "normalized_data": {"labor": []},
    }

    monkeypatch.setattr(
        module,
        "IntelligentExtractor",
        lambda: type("_", (), {"process_document_intelligently": lambda *_: fake_result}),
    )

    result = module.lambda_handler(_build_event("invoice.pdf"), None)

    chunks = result["semantic_chunks"]
    first, second = chunks
    assert first["chunk_metadata"]["end_index"] > second["chunk_metadata"]["start_index"]


def test_fallback_basic_extraction_handles_gc(load_extraction, monkeypatch):
    module = load_extraction()

    fallback_result = {
        "extraction_status": "completed",
        "extracted_data": {"tables": [], "sheets": []},
        "semantic_chunks": [],
        "processing_summary": {"processing_method": "fallback", "total_chunks": 0},
        "normalized_data": {"labor": []},
    }

    class FakeExtractor:
        def __init__(self, *_: Any, **__: Any) -> None:
            raise RuntimeError("Intelligent path disabled")

    monkeypatch.setattr(module, "IntelligentExtractor", FakeExtractor)
    monkeypatch.setattr(module, "handle_basic_extraction_fallback", lambda _: fallback_result)

    result = module.lambda_handler(_build_event("invoice.pdf"), None)

    assert result["processing_summary"]["processing_method"] == "fallback"
    assert result["extraction_status"] == "completed"
    assert result["semantic_chunks"] == []
