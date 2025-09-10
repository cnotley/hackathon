import importlib, pytest


@pytest.mark.skipif(importlib.util.find_spec("streamlit") is None, reason="streamlit not installed")
def test_streamlit_imports():
    mod = importlib.import_module("ui.app")
    assert hasattr(mod, "main")
