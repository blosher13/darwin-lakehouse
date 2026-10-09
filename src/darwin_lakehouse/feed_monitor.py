#
# Darwin Push Port feed monitor - a lightweight STOMP client for checking the live feed.
#
# Derived from the National Rail Open Data client demonstrator,
# Copyright (C) 2019-2024 OpenTrainTimes Ltd., licensed under the GNU GPL v3 or later.
# Modernised: settings from .env, no PyXB (standard-library XML parsing), per-type counters.
#
"""Connects to Darwin over STOMP and logs a one-line summary of every message.

The production pipeline ingests through Kafka Connect; this is a quick way to confirm
credentials and see what the feed looks like.

    darwin-feed-monitor [--seconds 60]
"""

from __future__ import annotations

import argparse
import collections
import logging
import socket
import time
import xml.etree.ElementTree as ET
import zlib

import stomp

from darwin_lakehouse.config import Settings

log = logging.getLogger("darwin_lakehouse.feed")

HOST = "darwin-dist-44ae45.nationalrail.co.uk"
PORT = 61613
TOPIC = "/topic/darwin.pushport-v16"
HEARTBEAT_MS = 15000


def summarise(body: bytes) -> str:
    """Decompresses a Push Port message and describes its first update element."""
    root = ET.fromstring(zlib.decompress(body, zlib.MAX_WBITS | 32))
    update = next(iter(root), None)
    if update is None:
        return f"empty Pport ts={root.get('ts')}"
    elements = [el.tag.rsplit("}", 1)[-1] for el in update]
    first = update[0] if len(update) else None
    detail = f" rid={first.get('rid')}" if first is not None and first.get("rid") else ""
    return f"{update.tag.rsplit('}', 1)[-1]} {','.join(elements)}{detail} origin={update.get('updateOrigin')}"


class FeedListener(stomp.ConnectionListener):
    def __init__(self):
        self.counts: collections.Counter[str] = collections.Counter()

    def on_message(self, frame):
        message_type = frame.headers.get("MessageType", "?")
        self.counts[message_type] += 1
        try:
            log.info("seq=%s type=%s %s", frame.headers.get("SequenceNumber"), message_type, summarise(frame.body))
        except (zlib.error, ET.ParseError) as e:
            log.error("Could not decode message: %s", e)

    def on_error(self, frame):
        log.error("Broker error: %s", frame.body)

    def on_disconnected(self):
        log.warning("Disconnected")


def main() -> None:
    logging.basicConfig(format="%(asctime)s %(levelname)s\t%(message)s", level=logging.INFO)
    logging.getLogger("stomp").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description="Watch the live Darwin Push Port feed")
    parser.add_argument("--seconds", type=int, default=60, help="how long to listen (default 60)")
    args = parser.parse_args()

    settings = Settings()
    username = settings.require("username")
    listener = FeedListener()
    conn = stomp.Connection12([(HOST, PORT)], auto_decode=False, heartbeats=(HEARTBEAT_MS, HEARTBEAT_MS))
    conn.set_listener("", listener)
    # Non-durable subscription: monitoring must not leave messages queued on Darwin's broker
    conn.connect(username=username, passcode=settings.require("national_rail_password"), wait=True,
                 headers={"client-id": f"{username}-monitor-{socket.gethostname()}"})
    conn.subscribe(destination=TOPIC, id="1", ack="auto")
    try:
        time.sleep(args.seconds)
    except KeyboardInterrupt:
        pass
    finally:
        conn.disconnect()
    log.info("Messages by type: %s", dict(listener.counts.most_common()))


if __name__ == "__main__":
    main()
