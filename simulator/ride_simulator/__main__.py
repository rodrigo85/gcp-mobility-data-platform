"""Simulator CLI: publishes ride events to Pub/Sub.

With PUBSUB_EMULATOR_HOST set, the client publishes to the local emulator
instead of real Pub/Sub.

    python -m ride_simulator --project my-project --topic ride-events --rate 20
"""

from __future__ import annotations

import argparse
import heapq
import logging
import random
import time

from ride_simulator.events import ChaosConfig, Publishable, simulate_ride, to_messages, utcnow

log = logging.getLogger("ride_simulator")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--rate", type=float, default=10.0, help="rides started per second")
    p.add_argument("--duration-seconds", type=int, default=60)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--late-fraction", type=float, default=0.02)
    p.add_argument("--duplicate-fraction", type=float, default=0.01)
    p.add_argument("--malformed-fraction", type=float, default=0.005)
    p.add_argument("--dry-run", action="store_true", help="print instead of publishing")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    rng = random.Random(args.seed)
    chaos = ChaosConfig(
        late_fraction=args.late_fraction,
        duplicate_fraction=args.duplicate_fraction,
        malformed_fraction=args.malformed_fraction,
    )

    publish = _make_publisher(args)
    # Queue of delayed messages, ordered by when they are due
    delayed: list[tuple[float, int, Publishable]] = []
    seq = sent = 0
    # Fractional rates (0.5 = one ride every 2 s) accumulate across ticks
    ride_budget = 0.0
    deadline = time.monotonic() + args.duration_seconds

    while time.monotonic() < deadline:
        tick = time.monotonic()
        ride_budget += args.rate
        rides_now = int(ride_budget)
        ride_budget -= rides_now
        for _ in range(rides_now):
            for msg in to_messages(simulate_ride(utcnow(), rng, chaos), rng, chaos):
                delay = float(msg.attributes.pop("publish_delay_s", 0))
                if delay:
                    seq += 1
                    heapq.heappush(delayed, (tick + delay, seq, msg))
                else:
                    publish(msg)
                    sent += 1
        while delayed and delayed[0][0] <= time.monotonic():
            publish(heapq.heappop(delayed)[2])
            sent += 1
        time.sleep(max(0.0, 1.0 - (time.monotonic() - tick)))

    log.info("published %d messages (%d delayed messages dropped at the end)", sent, len(delayed))


def _make_publisher(args: argparse.Namespace):
    if args.dry_run:
        return lambda msg: print(msg.data.decode(), msg.attributes)

    from google.cloud import pubsub_v1

    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(args.project, args.topic)
    return lambda msg: client.publish(topic_path, msg.data, **msg.attributes)


if __name__ == "__main__":
    main()
