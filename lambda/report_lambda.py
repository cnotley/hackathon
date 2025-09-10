import os, io, logging, tempfile, json, shutil, subprocess
from datetime import datetime
import openpyxl
from openpyxl.styles import PatternFill
from PyPDF2 import PdfReader, PdfWriter
from layers.common.python.common import client

def _markdown_from_flags(extracted, comparison):
    lines = []
    lines.append("# Audit Report"); lines.append("")
    lines.append(f"- **Invoice**: {extracted.get('invoice_number') or '(unknown)'}")
    lines.append(f"- **Project**: {extracted.get('project') or '(n/a)'}")
    lines.append(f"- **Loss date**: {extracted.get('loss_date') or '(n/a)'}")
    lines.append(""); lines.append("## Flags")
    if not comparison.get("flags"):
        lines.append("No issues detected.")
    else:
        for f in comparison["flags"]:
            lines.append(f"- `{f['type']}`: {json.dumps({k:v for k,v in f.items() if k!='type'})}")
    lines.append(""); lines.append(f"**Estimated Savings**: ${comparison.get('estimated_savings',0):,}")
    return "\n".join(lines)


def _generate_markdown_report(extracted, comparison):
    """Use Bedrock to generate a richer markdown report, fallback to template."""
    base = _markdown_from_flags(extracted, comparison)
    if os.environ.get("USE_BEDROCK", "").lower() != "true":
        return base
    try:
        br = client("bedrock-runtime")
        prompt = json.dumps(
            {
                "extracted": extracted,
                "comparison": comparison,
                "instruction": "Generate a concise markdown audit report",
            }
        )
        delay = 1.0
        for _ in range(3):
            try:
                resp = br.invoke_model(modelId="markdown-writer", body=prompt)
                body = resp.get("body") or resp.get("Body")
                if body:
                    return body.read().decode("utf-8") if hasattr(body, "read") else body
            except Exception as e:  # pragma: no cover - network failures
                logging.warning("bedrock report failed: %s", e)
                time.sleep(delay)
                delay *= 1.5
    except Exception:
        pass
    return base

def _excel_from_data(extracted, comparison, path):
    wb = openpyxl.Workbook()
    ws = wb.active; ws.title = "Project Information"
    ws["A1"] = "Project Name"; ws["B1"] = extracted.get("project") or ""
    ws["A2"] = "Project Number"; ws["B2"] = extracted.get("invoice_number") or ""
    ws["A3"] = "Loss Date"; ws["B3"] = extracted.get("loss_date") or "02/12/2025"
    ws["A4"] = "Cause"; ws["B4"] = "Fire/Water"
    ws["A5"] = "Currency"; ws["B5"] = extracted.get("currency", "USD")

    ws2 = wb.create_sheet("Project Summary")
    ws2.append(["Category","As Presented","Analyzed","Hold/Reduction"])
    presented = {
        "labor": extracted.get("summary", {}).get("labor", 0),
        "consumables": extracted.get("summary", {}).get("consumables", 0),
        "equipment": extracted.get("summary", {}).get("equipment", 0),
        "subcontractors": extracted.get("summary", {}).get("subcontractors", 0),
        "misc": extracted.get("summary", {}).get("misc", 0),
        "tax": extracted.get("summary", {}).get("tax", 0),
    }
    holds = {k: 0 for k in presented}
    for f in comparison.get("flags", []):
        if f["type"] in ("rate_high_vs_mwo", "anomaly", "duplicate_line"):
            holds["labor"] += 100
    for k in presented:
        ws2.append([k, presented[k], presented[k], holds[k]])

    ws3 = wb.create_sheet("Labor Export")
    ws3.append(["Name","Type","Code","Rate","Hours","Total"])
    for row in extracted.get("labor", []):
        ws3.append([row.get("name"), row.get("type"), row.get("code"), row.get("rate"), row.get("total_hours"), row.get("total")])

    red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    for r in range(2, ws2.max_row + 1):
        presented_val = ws2.cell(r, 2).value or 0
        hold_val = ws2.cell(r, 4).value or 0
        pct = (hold_val / presented_val) if presented_val else 0
        fill = None
        if pct >= 0.10:
            fill = red
        elif pct >= 0.05:
            fill = yellow
        if fill:
            for c in range(1, 5):
                ws2.cell(r, c).fill = fill
    wb.save(path)

def _maybe_wkhtmltopdf(markdown_text, pdf_path):
    bin_path = os.environ.get("WKHTMLTOPDF_BIN") or shutil.which("wkhtmltopdf")
    if not bin_path or not os.path.exists(bin_path):
        return False
    try:
        import markdown as md
        html = md.markdown(markdown_text)
    except Exception:
        html = f"<pre>{markdown_text}</pre>"
    tmp_html = tempfile.NamedTemporaryFile(delete=False, suffix=".html").name
    with open(tmp_html, "w", encoding="utf-8") as f:
        f.write(html)
    try:
        subprocess.run([bin_path, "--page-size", "Letter", tmp_html, pdf_path], check=True)
        return True
    except Exception:
        return False
    finally:
        os.unlink(tmp_html)


def _watermark_pdf(path, text):
    try:
        from reportlab.pdfgen import canvas
    except Exception:  # pragma: no cover - optional dependency
        return
    packet = io.BytesIO()
    can = canvas.Canvas(packet)
    can.setFont("Helvetica", 40)
    try:
        can.setFillAlpha(0.2)
    except Exception:
        pass
    can.drawCentredString(300, 400, text)
    can.save()
    packet.seek(0)
    watermark = PdfReader(packet)
    reader = PdfReader(path)
    writer = PdfWriter()
    for page in reader.pages:
        page.merge_page(watermark.pages[0])
        writer.add_page(page)
    with open(path, "wb") as f:
        writer.write(f)


def _secure_pdf(path, password):
    reader = PdfReader(path)
    writer = PdfWriter()
    for p in reader.pages:
        writer.add_page(p)
    writer.encrypt(password)
    with open(path, "wb") as f:
        writer.write(f)

def generate_report(extracted, comparison, out_bucket=None, out_key_prefix="reports/"):
    md = _generate_markdown_report(extracted, comparison)
    tmp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
    _excel_from_data(extracted, comparison, tmp_xlsx)
    outputs = {"report.md": md}
    with open(tmp_xlsx, "rb") as f:
        outputs["report.xlsx"] = f.read()

    tmp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name
    if _maybe_wkhtmltopdf(md, tmp_pdf):
        _watermark_pdf(tmp_pdf, os.environ.get("PDF_WATERMARK", "CONFIDENTIAL"))
        if os.environ.get("PDF_PASSWORD"):
            _secure_pdf(tmp_pdf, os.environ["PDF_PASSWORD"])
        with open(tmp_pdf, "rb") as f:
            outputs["report.pdf"] = f.read()

    if out_bucket:
        import boto3
        s3 = boto3.client("s3")
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        base = f"{out_key_prefix}{extracted.get('invoice_number','inv')}_{ts}"
        s3.put_object(Bucket=out_bucket, Key=base + ".md", Body=md.encode("utf-8"))
        s3.put_object(Bucket=out_bucket, Key=base + ".xlsx", Body=outputs["report.xlsx"])
        if "report.pdf" in outputs:
            s3.put_object(Bucket=out_bucket, Key=base + ".pdf", Body=outputs["report.pdf"])
        return {"s3": {"bucket": out_bucket, "prefix": base}, "generated": list(outputs.keys())}

    return {"generated": list(outputs.keys()), "files": outputs}

def generate_handler(event, context):
    extracted = event.get("extracted") or event
    comparison = event.get("comparison") or {}
    return generate_report(extracted, comparison, os.environ.get("REPORTS_BUCKET"))
