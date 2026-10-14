import base64
import json
import sys
import types
from types import SimpleNamespace

import pytest

# functions_framework is only needed at deploy time; stub its decorator for tests
sys.modules.setdefault("functions_framework", types.SimpleNamespace(cloud_event=lambda fn: fn))

import main  # noqa: E402


@pytest.mark.parametrize(
    ("cost", "budget", "expected"),
    [
        (0.0, 100.0, False),
        (69.99, 100.0, False),
        (70.0, 100.0, True),
        (150.0, 100.0, True),
        (10.0, 0.0, False),
    ],
)
def test_should_disable_at_kill_fraction(cost, budget, expected):
    notification = {"costAmount": cost, "budgetAmount": budget}
    assert main.should_disable(notification, kill_fraction=0.7) is expected


def event(cost: float, budget: float = 100.0):
    data = json.dumps({"costAmount": cost, "budgetAmount": budget, "currencyCode": "BRL"})
    return SimpleNamespace(data={"message": {"data": base64.b64encode(data.encode())}})


def test_handler_disables_billing_above_threshold(monkeypatch):
    calls = []
    monkeypatch.setattr(main, "disable_billing", calls.append)
    monkeypatch.setattr(main, "_project_id", lambda: "my-project")
    monkeypatch.setenv("KILL_SWITCH_FRACTION", "0.7")
    monkeypatch.setenv("DRY_RUN", "false")

    main.handle_budget_alert(event(cost=10.0))
    assert calls == []

    main.handle_budget_alert(event(cost=75.0))
    assert calls == ["my-project"]


def test_dry_run_never_disables(monkeypatch):
    calls = []
    monkeypatch.setattr(main, "disable_billing", calls.append)
    monkeypatch.setenv("DRY_RUN", "true")

    main.handle_budget_alert(event(cost=500.0))
    assert calls == []
