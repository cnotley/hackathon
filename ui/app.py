import os, tempfile
import boto3
import streamlit as st
import importlib

agent_mod = importlib.import_module("lambda.agent_lambda")
report_mod = importlib.import_module("lambda.report_lambda")
invoke_agent = agent_mod.invoke_agent
_markdown_from_flags = report_mod._markdown_from_flags
_excel_from_data = report_mod._excel_from_data

def _save_uploaded_file(uploaded_file):
    path = os.path.join(tempfile.gettempdir(), uploaded_file.name)
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path


def _aws_credentials_ok() -> bool:
    try:
        boto3.client("sts").get_caller_identity()
        return True
    except Exception:  # pragma: no cover - credentials missing
        return False

def main():
    st.set_page_config(page_title="DR Invoice Auditor")
    st.title("Disaster Recovery Invoice Auditor")

    if not _aws_credentials_ok():
        st.error("Invalid AWS credentials. Configure and reload.")
        return

    user = st.text_input("User")
    pwd = st.text_input("Password", type="password")
    if os.environ.get("APP_PASSWORD") and pwd != os.environ["APP_PASSWORD"]:
        st.stop()

    up = st.file_uploader("Upload invoice PDF", type=["pdf"])
    if up and up.size > 5 * 1024 * 1024:
        st.error("File too large (>5MB)")
        return

    query = st.text_input("Agent query", "audit")
    run = st.button("Audit", type="primary")

    if run and up is not None:
        path = _save_uploaded_file(up)
        with st.spinner("Running pipeline..."):
            try:
                res = invoke_agent({"action": "audit", "local_path": path})
            except Exception as e:
                st.error(f"Pipeline failed: {e}")
                return
        extracted, comparison = res["extracted"], res["comparison"]
        st.session_state["last_extracted"] = extracted
        st.session_state["last_comparison"] = comparison
        st.subheader("Flags")
        if not comparison["flags"]:
            st.success("No issues detected")
        else:
            for f in comparison["flags"]:
                st.warning(str(f))
        st.metric("Estimated Savings", f"${comparison['estimated_savings']:,}")
        md_text = _markdown_from_flags(extracted, comparison)
        st.download_button("Download Markdown", data=md_text.encode("utf-8"), file_name="audit_report.md")
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
        _excel_from_data(extracted, comparison, tmp)
        with open(tmp, "rb") as f:
            st.download_button("Download Excel", data=f.read(), file_name="audit_report.xlsx")
        del st.session_state["last_extracted"]
        del st.session_state["last_comparison"]

if __name__ == "__main__":
    main()
