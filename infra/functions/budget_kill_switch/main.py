"""Budget kill switch: unlinks the billing account when spend crosses the cap.

Triggered by Cloud Billing budget notifications on Pub/Sub. Once billing is
unlinked, every paid service in the project stops. Data in BigQuery and Cloud
Storage is kept, but Google may delete resources if billing stays disabled
for a long time. Re-link the billing account in the console to resume.
"""

from __future__ import annotations

import base64
import json
import logging
import os

import functions_framework

log = logging.getLogger("budget_kill_switch")
logging.basicConfig(level=logging.INFO)


def should_disable(notification: dict, kill_fraction: float) -> bool:
    """True when the cost in the notification reached kill_fraction of the budget."""
    cost = float(notification.get("costAmount", 0))
    budget = float(notification.get("budgetAmount", 0))
    if budget <= 0:
        return False
    return cost >= budget * kill_fraction


def _project_id() -> str:
    import google.auth

    _, project = google.auth.default()
    return project


def disable_billing(project_id: str) -> None:
    from google.cloud import billing_v1

    client = billing_v1.CloudBillingClient()
    name = f"projects/{project_id}"
    info = client.get_project_billing_info(name=name)
    if not info.billing_enabled:
        log.info("billing already disabled for %s", project_id)
        return
    client.update_project_billing_info(
        name=name, project_billing_info=billing_v1.ProjectBillingInfo(billing_account_name="")
    )
    log.warning("billing DISABLED for %s", project_id)


@functions_framework.cloud_event
def handle_budget_alert(cloud_event) -> None:
    payload = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    notification = json.loads(payload)
    kill_fraction = float(os.environ.get("KILL_SWITCH_FRACTION", "0.7"))
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"

    log.info(
        "budget %s: cost %s of %s %s",
        notification.get("budgetDisplayName"),
        notification.get("costAmount"),
        notification.get("budgetAmount"),
        notification.get("currencyCode"),
    )
    if not should_disable(notification, kill_fraction):
        return
    if dry_run:
        log.warning("DRY_RUN: would disable billing (threshold %.0f%%)", kill_fraction * 100)
        return
    disable_billing(_project_id())
