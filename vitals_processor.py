"""
vitals_processor.py — Wrap vitals_standalone.process_vitals() for Kafka pipeline.

Input:  full device JSON dict (same format as training_data_200hz files)
Output: structured MongoDB document dict

Signal source priority (matches vitals_standalone._extract_pleth):
  1. PlethWave[32:]  — NISO204 / Berry device @ 120 Hz
  2. Pleth           — Berry watch PlethWave Byte12 @ 100/200 Hz
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import vitals_standalone as _vs
import kafka_config as cfg

log = logging.getLogger(__name__)


def process(msg: dict) -> dict:
    """
    Process one PPG vitals message from Kafka.

    Args:
        msg: Full device JSON dict. Required fields:
             PlethWave OR Pleth, Age, Gender
             Optional: PRAllData, BMI, Height, Weight,
                       Reference_SBP, Reference_DBP, offsets,
                       admissionId, patientId, facilityId, deviceId

    Returns:
        MongoDB document dict (ready for insert_one).
        Never raises — returns error document on failure.
    """
    t_start = time.time()

    admission_id = msg.get("admissionId", msg.get("AdmissionId", "UNKNOWN"))
    patient_id   = msg.get("patientId",   msg.get("PatientId",   msg.get("PatId", "UNKNOWN")))
    facility_id  = msg.get("facilityId",  cfg.FACILITY_ID)
    device_id    = msg.get("deviceId",    msg.get("DeviceId",    msg.get("BLEDeviceID", "UNKNOWN")))
    timestamp    = msg.get("timestamp",   int(datetime.now(timezone.utc).timestamp() * 1000))

    # Determine signal source for logging
    pw = msg.get("PlethWave", [])
    if len(pw) > 32:
        signal_source = "PlethWave[32:]"
        n_samples     = len(pw) - 32
        source_hz     = 120
    elif msg.get("Pleth"):
        signal_source = "Pleth"
        n_samples     = len(msg["Pleth"])
        source_hz     = msg.get("FS", 100)
    else:
        signal_source = "none"
        n_samples     = 0
        source_hz     = 120

    log.info(
        f"{admission_id} | signal={signal_source} "
        f"samples={n_samples} hz={source_hz} "
        f"age={msg.get('Age')} gender={msg.get('Gender')}"
    )

    if n_samples < cfg.MIN_SAMPLES:
        log.warning(f"{admission_id} | Insufficient signal: {n_samples} samples (need {cfg.MIN_SAMPLES})")
        return _error_doc(
            admission_id, patient_id, facility_id, device_id, timestamp,
            f"Insufficient signal: {n_samples} samples (need {cfg.MIN_SAMPLES})"
        )

    try:
        result = _vs.process_vitals(msg, source_hz=msg.get("Source_HZ"))
    except Exception as exc:
        log.error(f"{admission_id} | process_vitals raised: {exc}", exc_info=True)
        return _error_doc(admission_id, patient_id, facility_id, device_id, timestamp, str(exc))

    elapsed = round(time.time() - t_start, 2)

    if result.get("status") != "success":
        msg_txt = result.get("message", "Inference failed")
        log.warning(f"{admission_id} | Inference failed: {msg_txt}")
        return _error_doc(admission_id, patient_id, facility_id, device_id, timestamp, msg_txt)

    sbp      = result["sbp"]
    dbp      = result["dbp"]
    category = result["category"]
    hb       = result["hb"]
    glucose  = result["glucose"]
    offsets  = result.get("active_offsets", {})
    meta     = result.get("metadata", {})

    log.info(
        f"{admission_id} | BP={sbp}/{dbp} ({category}) "
        f"Hb={hb} Glu={glucose} offsets={offsets} {elapsed}s"
    )

    return {
        "uuid":        str(uuid.uuid4()),
        "admissionId": admission_id,
        "patientId":   patient_id,
        "facilityId":  facility_id,
        "deviceId":    device_id,
        "timestamp":   timestamp,

        "input": {
            "age":              msg.get("Age"),
            "gender":           msg.get("Gender"),
            "bmi":              msg.get("BMI"),
            "height_cm":        msg.get("Height"),
            "weight_kg":        msg.get("Weight"),
            "source_hz":        meta.get("source_hz", source_hz),
            "input_samples":    meta.get("input_samples", n_samples),
            "signal_source":    signal_source,
            "calibration_used": bool(
                msg.get("Reference_SBP") or msg.get("Reference_DBP")
            ),
        },

        "vitals": {
            "sbp":         sbp,
            "dbp":         dbp,
            "bp_category": category,
            "hb":          hb,
            "glucose":     glucose,
        },

        "offsets": offsets,

        "processingStatus": None,
        "processedAt":      None,
        "processedBy":      None,
        "_processing_time_s": elapsed,
        "_processed_utc":     datetime.now(timezone.utc).isoformat(),
    }


def _error_doc(admission_id, patient_id, facility_id, device_id, timestamp, reason):
    return {
        "uuid":        str(uuid.uuid4()),
        "admissionId": admission_id,
        "patientId":   patient_id,
        "facilityId":  facility_id,
        "deviceId":    device_id,
        "timestamp":   timestamp,
        "input":       {},
        "vitals":      None,
        "offsets":     {},
        "processingStatus": "error",
        "processingError":  reason,
        "processedAt":      None,
        "processedBy":      None,
        "_processed_utc":   datetime.now(timezone.utc).isoformat(),
    }
