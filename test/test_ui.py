import importlib

def test_streamlit_imports():
    mod = importlib.import_module("ui.app")
    assert hasattr(mod, "main")
