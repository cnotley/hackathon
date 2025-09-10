import importlib
import os
import pytest

extraction_lambda = importlib.import_module("lambda.extraction_lambda")
extract_data = extraction_lambda.extract_data

def test_extract_local_pdf():
    pdf_path = os.path.join(os.path.dirname(__file__), "..", "test_invoice.pdf")
    data = extract_data(local_path=pdf_path)
    assert data["invoice_number"] == "3034894"
    assert "labor" in data and data["summary"].get("labor") == 77150.25


def test_extract_xlsx_without_pandas(monkeypatch):
    monkeypatch.setattr(extraction_lambda, "pd", None)
    with pytest.raises(ImportError) as excinfo:
        extract_data(local_path="dummy.xlsx")
    assert "pip install pandas" in str(excinfo.value)
