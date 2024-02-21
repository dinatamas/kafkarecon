#!/usr/bin/env python3
"""
Reconnaissance and enumeration tool for Apache Kafka.
"""
import argparse
from dataclasses import dataclass
import json
import random
import shlex
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource


@dataclass
class State:
    args = argparse.Namespace()
    config = dict()
    admin: AdminClient | None = None
    consumer: Consumer | None = None
    broker = "not connected"


S = State()


def main():

    # ============================ #
    # Parse command line arguments #
    # ============================ #

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="load kafka configuration from json file")
    S.args = parser.parse_args()

    # ========================== #
    # Load initial configuration #
    # ========================== #

    print()
    if S.args.config:
        exec_load(S.config, S.args.config)
    else:
        print(" (+) Started without initial configuration")

    # ================= #
    # Main command loop #
    # ================= #

    while True:

        # ============== #
        # Command prompt #
        # ============== #

        print()
        print(f" ┌──({S.broker})")
        command = shlex.split(input(" └─$ "))
        if not command:
            continue
        print()
        if command[0] == "exit":
            break
        if command[0] in ("help", "?"):
            tprint(
                ["Command",        "Description"],
                # -------           -----------
                ["config",         "show current configuration"],
                ["connect",        "create consumer and admin client"],
                ["disconnect",     "close consumer and admin client"],
                ["exit",           "exit the script"],
                ["help",           "show this help message"],
                ["load <file>",    "load kafka config from json file"],
            )

        # ======================== #
        # Configuration management #
        # ======================== #

        elif command[0] == "config":
            print_config(S.config)
        elif command[0] == "load":
            if len(command) != 2:
                print(" (-) usage: load <file>")
                continue
            exec_load(S.config, command[1])

        # =================== #
        # Connection handling #
        # =================== #

        elif command[0] == "connect":
            if not S.config:
                print(" (-) Configuration required")
                continue
            exec_connect(S.config)
        elif command[0] == "disconnect":
            exec_disconnect(S.admin, S.consumer)

        # ================================ #
        # General enumeration and overview #
        # ================================ #

        elif command[0] == "cluster":
            exec_cluster(S.admin, S.consumer)

        # =============== #
        # Unknown command #
        # =============== #

        else:
            print(f" (-) Command not found: {command[0]}")


# ======================== #
# Configuration management #
# ======================== #

def exec_load(config, fpath):
    try:
        with open(fpath) as f:
            new = json.load(f)
    except Exception:
        print(f" (-) Could not load file: {fpath}")
        return
    if not isinstance(new, dict):
        print(f" (-) Configuration must be an object")
        return
    config |= new
    print(" (+) Loaded configuration from file:")
    print()
    print_config(new)


def print_config(config):
    if not config:
        print(" (-) No configuration")
        return
    tprint(["Key", "Value"], *config.items())


# =================== #
# Connection handling #
# =================== #


def exec_connect(config):

    # ======================== #
    # Preprocess configuration #
    # ======================== #

    if "group.id" not in config:
        config["group.id"] = gid = uuid4().hex
        print(f" (+) Group ID not configured, using: {gid}")
        print()

    if "bootstrap.servers" not in config:
        print(f" (-) Bootstrap server not configured")
        return

    bootstrap_server = config["bootstrap.servers"]
    if isinstance(bootstrap_server, list):
        bootstrap_server = random.choice(bootstrap_server)

    # ======================== #
    # Instantiate admin client #
    # ======================== #

    try:
        S.admin = AdminClient({
            **({k: v for k, v in config.items() if k in (
                "security.protocol",
                "ssl.ca.location",
                "ssl.certificate.location",
                "ssl.key.location",
            )}),
            "bootstrap.servers": bootstrap_server,
        })
        S.broker = bootstrap_server
        print(" (+) Admin client connected")
    except KafkaException as e:
        print(f" (-) Admin client connection failed: {str(e)}")

    # ==================== #
    # Instantiate consumer #
    # ==================== #

    print()
    try:
        S.consumer = Consumer({
            **config,
            "bootstrap.servers": bootstrap_server,
            "enable.partition.eof": True,
        })
        S.broker = bootstrap_server
        print(" (+) Consumer connected")
    except KafkaException as e:
        print(f" (-) Consumer connection failed: {str(e)}")


def exec_disconnect(admin, consumer):
    if admin is None and consumer is None:
        print(" (-) Not connected")
    if admin is not None:
        admin.close()
        print(" (+) Admin disconnected")
    if consumer is not None:
        consumer.close()
        print(" (+) Consumer disconnected")


# ================================ #
# General enumeration and overview #
# ================================ #

def exec_cluster(admin, consumer):

    if admin is None and consumer is None:
        print(f" (-) Not connected")
        return

    # ========== #
    # Fetch data #
    # ========== #

    cluster = None
    # requires confluent-kafka==2.3.0
    # try:
    #     if admin is not None:
    #         cluster = admin.describe_cluster(request_timeout=15)
    # except KafkaException as e:
    #     print(f" (-) Could not query cluster information: {str(e)}")
    #     print()

    meta = None
    try:
        if admin is not None:
            meta = admin.list_topics(timeout=15)
        elif consumer is not None:
            meta = consumer.list_topics(timeout=15)
    except KafkaException as e:
        print(f" (-) Could not query metadata: {str(e)}")
        print()

    # =================== #
    # Cluster information #
    # =================== #

    if meta is not None:
        print(f" (+) Cluster ID: {meta.cluster_id}")
        print()

    if cluster is not None:
        # requires confluent-kafka==2.3.0
        print(cluster)

    # =============== #
    # Broker metadata #
    # =============== #

    if meta is not None:
        print(f" (+) Metadata origin broker name: {meta.orig_broker_name}")
        print()

        brokers = sorted(meta.brokers.values(), key=lambda b: b.id)
        tprint(
            ["ID", "Host", "Port"],
            *([b.id, b.host, b.port] for b in brokers)
        )

        print()
        if meta.orig_broker_id in meta.brokers:
            print(f" (+) Metadata origin broker ID: {meta.orig_broker_id}")
        else:
            print(f" (-) Invalid metadata origin broker ID: {meta.orig_broker_id}")
        print()
        if meta.controller_id in meta.brokers:
            print(f" (+) Controller broker ID: {meta.controller_id}")
        else:
            print(f" (-) Invalid controller broker ID: {meta.controller_id}")
        print()

    # ======================= #
    # Broker resource configs #
    # ======================= #

    if admin is not None and meta is not None:
        for broker in meta.brokers.values():
            res = ConfigResource(ConfigResource.Type.BROKER, str(broker.id))
            try:
                entries = admin.describe_configs([res])[res].result()
                entries = dict(sorted(entries.items()))
                tprint(
                    ["Name", "Value", "Source", "Read Only", "Sensitive"],
                    *(
                        [
                            e.name[:40],
                            e.value[:20] if e.value is not None else "-",
                            e.source,
                            "Yes" if e.is_read_only else "",
                            "Yes" if e.is_sensitive else "",
                        ] for k, e in entries.items() if k in (
                            "ssl.client.auth"
                        )
                    )
                )
            except KafkaException as e:
                print(f" (-) Could not describe broker {broker.id}")


# ================================ #
# Command line interface utilities #
# ================================ #


def tprint(*rows):
    if isinstance(rows[0], str):
        rows = [[rows[0]], *[[r] for r in rows[1:]]]
    newrows = []
    for row in rows:
        newcols = [cell if isinstance(cell, list) else [cell] for cell in row]
        longest = max(len(col) for col in newcols)
        newcols = [col + [" ..."] * (longest - len(col)) for col in newcols]
        for i in range(longest):
            newcells = [str(col[i]) for col in newcols]
            newrows.append(newcells)
    rows = newrows
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    for row in [rows[0], ["-" * len(header) for header in rows[0]], *rows[1:]]:
        print("   " + "  ".join(cell.ljust(width) for cell, width in zip(row, widths)))


# ================= #
# Script entrypoint #
# ================= #


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print()
    except Exception as e:
        print(f" (-) ERROR: {e}")
