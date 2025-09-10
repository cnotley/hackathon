from lambda.agent_lambda import invoke_agent
import os

def test_agent_pipeline():
    pdf_path = os.path.join(os.path.dirname(__file__), "..", "test_invoice.pdf")
    res = invoke_agent({"action":"audit","local_path": pdf_path})
    assert "extracted" in res and "comparison" in res
