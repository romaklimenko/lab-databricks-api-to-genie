"""Compare held-out Genie query results with independent reference SQL."""

from decimal import Decimal, InvalidOperation


def normalized(rows):
    def cell(value):
        if value is None:
            return None
        try:
            return str(Decimal(str(value)).quantize(Decimal("0.001")))
        except InvalidOperation:
            return str(value)

    return sorted([tuple(cell(value) for value in row) for row in rows], key=str)


def compare(name, reference, answer):
    expected = reference.get("result", {}).get("data_array", [])
    results = [item.get("statement_response", {}) for item in answer.get("data", [])]
    text = " ".join(answer.get("text", [])).lower()
    # SQL comments and aliases are evidence for result units when Genie returns a table only.
    sql = " ".join(answer.get("queries", [])).lower()
    checks = {"reference_succeeded": reference.get("status", {}).get("state") == "SUCCEEDED"}
    if name == "missing_period":
        empty_result = any(
            result.get("status", {}).get("state") == "SUCCEEDED"
            and not result.get("result", {}).get("data_array", [])
            for result in results
        )
        explicit_missing = any(
            term in text
            for term in (
                "no data",
                "no records",
                "no weather data",
                "not available",
                "does not cover",
                "outside",
                "no rows",
            )
        )

        def diagnostic_only(result):
            data = result.get("result", {}).get("data_array", [])
            columns = result.get("manifest", {}).get("schema", {}).get("columns", [])
            return (
                len(columns) == 1
                and columns[0].get("name") == "message"
                and columns[0].get("type_name") == "STRING"
                and len(data) == 1
                and len(data[0]) == 1
                and "no data coverage" in str(data[0][0]).lower()
            )

        # Genie may return a single diagnostic message row instead of an empty weather table.
        has_weather_rows = any(
            result.get("result", {}).get("data_array") and not diagnostic_only(result)
            for result in results
        )
        checks["missing_coverage"] = (
            not expected and (empty_result or explicit_missing) and not has_weather_rows
        )
    else:
        checks["rows_match"] = any(
            result.get("status", {}).get("state") == "SUCCEEDED"
            and normalized(result.get("result", {}).get("data_array", [])) == normalized(expected)
            for result in results
        )
        units = {
            "precipitation": ("millimeter", "millimetre", "_mm"),
            "temperature": ("celsius", "°c", "_c"),
            "wind": ("meters per second", "metres per second", "m/s", "_ms"),
        }
        if name in units:
            checks["unit_present"] = any(unit in text + " " + sql for unit in units[name])
    return {"passed": all(checks.values()), "checks": checks}
