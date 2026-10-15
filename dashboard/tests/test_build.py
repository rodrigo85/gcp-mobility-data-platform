import json
import re
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import build

TEMPLATE = Path(__file__).resolve().parents[1] / "template.html"


def sample() -> dict[str, list[dict]]:
    return {key: [{"id": 1, "label": key}] for key in build.TABLES}


def embedded(html: str) -> dict:
    match = re.search(r"const DATA = (.*?);\n", html, re.DOTALL)
    assert match, "the embedded data was not found"
    return json.loads(match.group(1).replace("<\\/", "</"))


def test_render_embeds_the_data_and_the_date():
    html = build.render(TEMPLATE.read_text(encoding="utf-8"), sample(), date(2026, 10, 15))
    assert embedded(html) == sample()
    assert "October 15, 2026" in html
    assert "__DATA__" not in html
    assert "__BUILT_AT__" not in html


def test_values_cannot_close_the_script_element():
    data = sample()
    data["kpis"] = [{"note": "</script><script>alert(1)</script>"}]
    html = build.render(TEMPLATE.read_text(encoding="utf-8"), data, date(2026, 10, 15))
    assert html.count("</script>") == 2  # the d3 tag and the page script, nothing injected
    assert embedded(html)["kpis"] == data["kpis"]


def test_bigquery_cells_become_json_friendly():
    assert build._plain(Decimal("12.50")) == 12.5
    assert build._plain(date(2025, 1, 5)) == "2025-01-05"
    assert build._plain("text") == "text"


def test_template_needs_each_placeholder_once():
    with pytest.raises(ValueError, match="__DATA__"):
        build.render("<p>no placeholders</p>", sample(), date(2026, 10, 15))


def test_json_input_must_cover_every_mart(tmp_path):
    path = tmp_path / "marts.json"
    path.write_text(json.dumps({"daily": []}), encoding="utf-8")
    with pytest.raises(SystemExit, match="missing datasets"):
        build.load_json(path)


def test_main_writes_the_page(tmp_path):
    source = tmp_path / "marts.json"
    source.write_text(json.dumps(sample()), encoding="utf-8")
    out = tmp_path / "index.html"
    assert build.main(["--from-json", str(source), "--output", str(out)]) == 0
    assert embedded(out.read_text(encoding="utf-8")) == sample()


def test_page_waits_for_d3_instead_of_assuming_script_order():
    # htmlpreview.github.io runs the inline script before the d3 <script> finishes loading
    html = build.render(TEMPLATE.read_text(encoding="utf-8"), sample(), date(2026, 10, 15))
    assert "waitForD3" in html
    assert "typeof d3" in html
