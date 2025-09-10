import base64
import json
import time
from typing import Any, Dict

import boto3
import pandas as pd
import streamlit as st


def _invoke_agent(file_bytes: bytes, query: str) -> Dict[str, Any]:
    client = boto3.client('lambda')
    payload = {'file': base64.b64encode(file_bytes).decode(), 'query': query}
    resp = client.invoke(FunctionName='agent_lambda', Payload=json.dumps(payload))
    data = resp['Payload'].read()
    return json.loads(data or '{}')


def main() -> None:
    st.title('Invoice Auditor')
    if 'user' not in st.session_state:
        st.session_state['user'] = ''
    st.session_state['user'] = st.text_input('Username', st.session_state['user'])

    file = st.file_uploader('Upload PDF/Excel', type=['pdf', 'xlsx'])
    query = st.text_input('Audit Query', 'Audit invoice for discrepancies')

    if st.button('Audit'):
        if not file:
            st.error('No file uploaded')
            return
        data = file.getvalue()
        if len(data) > 5 * 1024 * 1024:
            st.error('Too large')
            return
        with st.spinner('Processing...'):
            result = _invoke_agent(data, query)
            st.markdown('### Agent Response')
            st.write(result)
            flags = result.get('flags', [])
            if flags:
                st.markdown('### Flags')
                st.dataframe(pd.DataFrame(flags))


if __name__ == '__main__':
    main()
