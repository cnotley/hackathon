import importlib

extraction_lambda = importlib.import_module('lambda.extraction_lambda')


def test_extract_local(monkeypatch):
    monkeypatch.setenv('LOCAL', '1')

    class FakePage:
        def extract_tables(self):
            return [[['Manderville', 'RS', '77', '55']]]

    class FakePDF:
        pages = [FakePage()]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr(extraction_lambda.pdfplumber, 'open', lambda path: FakePDF())
    data = extraction_lambda.extract_data({'file_path': 'test_invoice.pdf'}, None)
    assert data['labor'][0]['name'] == 'Manderville'
