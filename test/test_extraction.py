import importlib
extract_data = importlib.import_module("lambda.extraction_lambda").extract_data
import os

def test_extract_local_pdf():
    pdf_path = os.path.join(os.path.dirname(__file__), "..", "test_invoice.pdf")
    data = extract_data(local_path=pdf_path)
    assert data["invoice_number"] == "3034894"
    assert "labor" in data and data["summary"].get("labor") == 77150.25
