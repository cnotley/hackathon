import importlib
from unittest.mock import patch

report_lambda = importlib.import_module('lambda.report_lambda')
generate_report = report_lambda.generate_report


@patch('lambda.report_lambda.s3.upload_file')
def test_generate_report(mock_upload):
    event = {'flags': [{'details': 'RS rate 10% high'}], 'bucket': 'b', 'key': 'k.xlsx'}
    result = generate_report(event, None)
    assert result['bucket'] == 'b'
    mock_upload.assert_called()
