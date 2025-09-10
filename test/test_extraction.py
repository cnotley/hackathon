from lambda.extraction_lambda import extract_data
import os

def test_extract_local_pdf():
    pdf_path = os.path.join(os.path.dirname(__file__), "..", "test_invoice.pdf")
    data = extract_data(local_path=pdf_path)
    assert data["invoice_number"] == "3034894"
    assert "labor" in data
