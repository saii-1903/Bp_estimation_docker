"""
vitals_kafka_consumer.py — Consume PPG vitals data from Kafka, process, write to MongoDB.

Designed for K8s deployment (mirrors arrhythmia kafka_consumer.py pattern):
  - Multi-threaded: KAFKA_WORKERS parallel processing threads (default 3)
  - Graceful SIGTERM shutdown
  - Structured stdout logging (CloudWatch / K8s compatible)
  - Manual offset commit after successful MongoDB write
  - On failure: log error, write error doc to MongoDB, commit offset (never block queue)

Kafka message format (same JSON as training_data_200hz files):
  {
    "admissionId":  "ADM819104078",
    "patientId":    "MRN54642783626",
    "facilityId":   "CF1315821527",
    "deviceId":     "BM-001",
    "timestamp":    1770912322923,
    "Age":          45,
    "Gender":       "Male",
    "BMI":          25.9,          // or Height + Weight
    "PlethWave":    [...],         // 3600 values (NISO204) or >32 raw + signal
    "PRAllData":    [...],         // 30 HR values (1 per second)
    "Reference_SBP": null,        // optional calibration cuff reading
    "Reference_DBP": null,
    "offsets":      {}             // optional carry-forward offsets
  }
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue, Empty

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import kafka_config as cfg
from vitals_processor import process
from vitals_mongo_writer import write_result

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_running = True

def _handle_sigterm(signum, frame):
    global _running
    log.info("SIGTERM received — finishing in-flight messages, then shutting down.")
    _running = False

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)

# ---------------------------------------------------------------------------
# Message handling
# ---------------------------------------------------------------------------

def _parse(raw: bytes) -> dict | None:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        log.warning(f"Bad message (not JSON): {exc}")
        return None


def _validate(msg: dict) -> bool:
    # Must have a signal source
    has_pleth = len(msg.get("PlethWave", [])) > 32 or bool(msg.get("Pleth"))
    if not has_pleth:
        log.warning(
            f"{msg.get('admissionId', '?')} | Missing signal: "
            "need PlethWave (len>32) or Pleth"
        )
        return False
    return True


def _handle_message(msg_dict: dict):
    """Process one message + write to MongoDB. Always returns (never raises)."""
    admission_id = msg_dict.get("admissionId", "UNKNOWN")
    try:
        doc = process(msg_dict)
        write_result(doc)
    except Exception as exc:
        log.error(f"{admission_id} | Unhandled error: {exc}", exc_info=True)
        # Write an error record so the UI can surface it
        try:
            from vitals_processor import _error_doc
            from datetime import datetime, timezone
            err_doc = _error_doc(
                admission_id,
                msg_dict.get("patientId", "UNKNOWN"),
                msg_dict.get("facilityId", cfg.FACILITY_ID),
                msg_dict.get("deviceId", "UNKNOWN"),
                msg_dict.get("timestamp", int(datetime.now(timezone.utc).timestamp() * 1000)),
                str(exc),
            )
            write_result(err_doc)
        except Exception:
            pass   # logging already captured it


# ---------------------------------------------------------------------------
# Consumer loop
# ---------------------------------------------------------------------------

def run():
    conf = {
        "bootstrap.servers":  cfg.KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           cfg.KAFKA_GROUP_ID,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([cfg.KAFKA_TOPIC])

    log.info(
        f"PPG Vitals Consumer started | "
        f"topic={cfg.KAFKA_TOPIC} group={cfg.KAFKA_GROUP_ID} "
        f"bootstrap={cfg.KAFKA_BOOTSTRAP_SERVERS} workers={cfg.KAFKA_WORKERS}"
    )

    # Work queue feeds the thread pool
    work_queue: Queue = Queue(maxsize=cfg.KAFKA_WORKERS * 2)
    commit_lock = threading.Lock()

    def worker():
        while _running or not work_queue.empty():
            try:
                kafka_msg, msg_dict = work_queue.get(timeout=1.0)
            except Empty:
                continue
            try:
                _handle_message(msg_dict)
            finally:
                with commit_lock:
                    consumer.commit(message=kafka_msg)
                work_queue.task_done()

    with ThreadPoolExecutor(max_workers=cfg.KAFKA_WORKERS,
                            thread_name_prefix="vitals-worker") as pool:
        for _ in range(cfg.KAFKA_WORKERS):
            pool.submit(worker)

        try:
            while _running:
                kafka_msg = consumer.poll(timeout=1.0)

                if kafka_msg is None:
                    continue

                if kafka_msg.error():
                    if kafka_msg.error().code() == KafkaError._PARTITION_EOF:
                        log.debug(f"EOF partition {kafka_msg.partition()}")
                        continue
                    raise KafkaException(kafka_msg.error())

                msg_dict = _parse(kafka_msg.value())
                if msg_dict is None or not _validate(msg_dict):
                    consumer.commit(message=kafka_msg)
                    continue

                # Block if queue is full (back-pressure)
                work_queue.put((kafka_msg, msg_dict))

        except KafkaException as exc:
            log.error(f"Kafka error: {exc}")
            sys.exit(1)
        finally:
            log.info("Draining work queue...")
            work_queue.join()
            consumer.close()
            log.info("PPG Vitals Consumer stopped.")


if __name__ == "__main__":
    run()
