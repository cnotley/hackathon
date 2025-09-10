import os, tempfile
import streamlit as st
from lambda.agent_lambda import invoke_agent
from lambda.report_lambda import generate_report, _markdown_from_flags, _excel_from_data

def _save_uploaded_file(uploaded_file):
    path = os.path.join(tempfile.gettempdir(), uploaded_file.name)
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path

def main():
    st.set_page_config(page_title="DR Invoice Auditor")
    st.title("Disaster Recovery Invoice Auditor")
    up = st.file_uploader("Upload invoice PDF", type=["pdf"])
    query = st.text_input("Agent query", "audit")
    run = st.button("Audit", type="primary")

    if run and up is not None:
        path = _save_uploaded_file(up)
        with st.spinner("Running pipeline..."):
            res = invoke_agent({"action":"audit","local_path": path})
        extracted, comparison = res["extracted"], res["comparison"]
        st.subheader("Flags")
        if not comparison["flags"]:
            st.success("No issues detected")
        else:
            for f in comparison["flags"]:
                st.write(f)
        st.metric("Estimated Savings", f"${comparison['estimated_savings']:,}")
        md_text = _markdown_from_flags(extracted, comparison)
        st.download_button("Download Markdown", data=md_text.encode("utf-8"), file_name="audit_report.md")
        import tempfile
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
        _excel_from_data(extracted, comparison, tmp)
        with open(tmp, "rb") as f:
            st.download_button("Download Excel", data=f.read(), file_name="audit_report.xlsx")

if __name__ == "__main__":
    main()
