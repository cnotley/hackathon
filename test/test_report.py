from lambda.report_lambda import generate_report

def test_report_outputs():
    extr = {"invoice_number":"3034894","project":"Liverpool","loss_date":"02/12/2025","summary":{"labor":77000},"labor":[{"name":"Manderville","type":"Restoration Specialist","code":"RS","rate":77,"total_hours":55,"total":4812}]}
    comp = {"flags":[{"type":"rate_high_vs_mwo","code":"RS","expected":70,"seen":77}], "estimated_savings": 16000}
    out = generate_report(extr, comp, out_bucket=None)
    assert "report.xlsx" in out["generated"]
    assert "report.md" in out["generated"]
