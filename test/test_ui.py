from unittest.mock import MagicMock, patch
import importlib

app = importlib.import_module('ui.app')


def test_invoke_agent():
    fake_client = MagicMock()
    fake_client.invoke.return_value = {'Payload': MagicMock(read=lambda: b'{}')}
    with patch('ui.app.boto3.client', return_value=fake_client):
        result = app._invoke_agent(b'data', 'query')
    assert result == {}
