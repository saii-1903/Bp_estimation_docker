"""
kafka_config.py — All configuration from environment variables (12-factor app).
Vitals estimation pipeline (PPG → BP / Hb / Glucose).
"""
import os

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",             "ppg-vitals-topic")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID",          "ppg-vitals-consumer-group")
KAFKA_WORKERS           = int(os.getenv("KAFKA_WORKERS", "3"))   # parallel processing threads

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
MONGO_URI        = os.getenv("MONGO_URI",        "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB",         "vitals_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "ppg_vitals_results")

# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------
SAMPLING_RATE_HZ    = 120          # PlethWave[32:] is always 120 Hz
MIN_SAMPLES         = SAMPLING_RATE_HZ * 25   # 3000 — minimum acceptable signal

# ---------------------------------------------------------------------------
# Deployment
# ---------------------------------------------------------------------------
FACILITY_ID = os.getenv("FACILITY_ID", "CF0000000000")
LOG_LEVEL   = os.getenv("LOG_LEVEL",   "INFO")
