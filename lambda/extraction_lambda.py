import os, io, time, logging, re, json
import pdfplumber
from layers.common.python.common import client, chunk_text, semantic_map

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

OCR_MIN_CONF = float(os.environ.get("OCR_MIN_CONF","0.8"))

def _textract_analyze(bucket, key):
    tx = client("textract")
    if key.lower().endswith(".pdf"):
        job = tx.start_document_analysis(
            DocumentLocation={"S3Object":{"Bucket": bucket, "Name": key}},
            FeatureTypes=["TABLES","FORMS"]
        )
        job_id = job["JobId"]
        delay = 1.0
        while True:
            resp = tx.get_document_analysis(JobId=job_id)
            status = resp["JobStatus"]
            if status in ("SUCCEEDED","FAILED","PARTIAL_SUCCESS"):
                return resp
            time.sleep(delay)
            delay = min(10.0, delay * 1.6)
    else:
        raise NotImplementedError("Non-PDF path not implemented in prototype.")
    return {}

def _local_pdf_fallback(local_path):
    text = []
    try:
        with pdfplumber.open(local_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                text.append(t)
    except Exception as e:
        logger.error("pdfplumber failed: %s", e)
    return "\n\n".join(text)

def _normalize_terms(data):
    candidates = ["labor","consumables","equipment","subcontractors","misc","tax"]
    mapped = {}
    for k, v in data.items():
        best, sim = semantic_map(k, candidates)
        mapped[best or k] = v
    return mapped

def _parse_from_text(text):
    data = {
        "invoice_number": None,
        "date": None,
        "project": None,
        "loss_date": None,
        "summary": {},
        "labor": [],
        "total": None,
        "confidence_ok": True,
        "chunks": chunk_text(text, 150, 0.2),
    }
    m = re.search(r"Invoice\s*#(\d+)", text, re.I)
    if m: data["invoice_number"] = m.group(1)
    m = re.search(r"Date\s*(\d{2}/\d{2}/\d{4})", text)
    if m: data["date"] = m.group(1)
    m = re.search(r"Project:\s*([\w\s-]+)", text)
    if m: data["project"] = m.group(1).strip()
    m = re.search(r"Loss:\s*(\d{2}/\d{2}/\d{4})", text)
    if m: data["loss_date"] = m.group(1)

    for cat in ["labor","consum","equip","sub","misc","tax"]:
        m = re.search(fr"{cat}\s*\$([\d,\.]+)", text, re.I)
        if m:
            val = float(m.group(1).replace(",",""))
            key = {"consum":"consumables","equip":"equipment","sub":"subcontractors"}.get(cat, cat)
            data["summary"][key] = val

    for m in re.finditer(r"([A-Za-z]+)\s+(RS|GL)\s*\$([\d\.]+)\s*(\d+)hrs\s*\$([\d,\.]+)", text, re.I):
        name, code, rate, hours, total = m.groups()
        data["labor"].append({
            "name": name,
            "type": "Restoration Specialist" if code.upper()=="RS" else "General Labor",
            "code": code.upper(),
            "rate": float(rate),
            "total_hours": float(hours),
            "total": float(total.replace(",","")),
        })

    m = re.search(r"Total \$([\d,\.]+)", text, re.I)
    if m:
        data["total"] = float(m.group(1).replace(",", ""))

    fields = [data["invoice_number"], data["date"], data["summary"].get("labor")]
    if any(x is None for x in fields):
        data["confidence_ok"] = False
    data["summary"] = _normalize_terms(data["summary"])
    return data

def extract_data(bucket=None, key=None, local_path=None):
    if local_path:
        text = _local_pdf_fallback(local_path)
        return _parse_from_text(text)

    try:
        resp = _textract_analyze(bucket, key)
        blocks = resp.get("Blocks", [])
        text = " ".join(b.get("Text","") for b in blocks if b.get("BlockType")=="LINE")
        data = _parse_from_text(text)
        confs = [b.get("Confidence", 100) for b in blocks if b.get("BlockType")=="WORD"]
        if confs and (sum(confs)/len(confs))/100.0 < OCR_MIN_CONF:
            data["confidence_ok"] = False
        return data
    except Exception as e:
        logger.warning("Textract path failed, using local fallback: %s", e)
        if bucket and key:
            s3 = client("s3")
            bio = io.BytesIO()
            s3.download_fileobj(bucket, key, bio)
            bio.seek(0)
            tmp = "/tmp/invoice.pdf"
            with open(tmp, "wb") as f:
                f.write(bio.read())
            return _parse_from_text(_local_pdf_fallback(tmp))
        raise

def extract_handler(event, context):
    return extract_data(event.get("bucket"), event.get("key"), event.get("local_path"))
