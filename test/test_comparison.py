import importlib
compare_data = importlib.import_module("lambda.comparison_lambda").compare_data

def test_compare_estimates():
    extracted = {
        "labor":[
            {"name":"Manderville","code":"RS","rate":77,"total_hours":55,"total":4812},
            {"name":"Helper","code":"GL","rate":40,"total_hours":30,"total":1200},
        ],
        "summary":{"labor":77000},
        "total":160356.28
    }
    res = compare_data(extracted)
    assert any(f["type"] == "rate_high_vs_mwo" for f in res["flags"])
    assert res["estimated_savings"] == 385.0
