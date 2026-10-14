"""Create the topic and subscription on the Pub/Sub emulator (idempotent).

On real GCP, these resources are created by Terraform (infra/terraform/pubsub.tf).

    PUBSUB_EMULATOR_HOST=127.0.0.1:8085 python -m ride_simulator.emulator \\
        --project local-project --topic ride-events --subscription ride-events-beam
"""

from __future__ import annotations

import argparse
import os
import sys

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--subscription", required=True)
    args = p.parse_args()

    if not os.environ.get("PUBSUB_EMULATOR_HOST"):
        sys.exit("PUBSUB_EMULATOR_HOST is not set: use Terraform on real GCP")

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic = publisher.topic_path(args.project, args.topic)
    subscription = subscriber.subscription_path(args.project, args.subscription)

    for create, kwargs in (
        (publisher.create_topic, {"name": topic}),
        (
            subscriber.create_subscription,
            {"name": subscription, "topic": topic, "ack_deadline_seconds": 60},
        ),
    ):
        try:
            create(request=kwargs)
            print("created:", kwargs["name"])
        except AlreadyExists:
            print("already exists:", kwargs["name"])


if __name__ == "__main__":
    main()
