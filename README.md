<<<<<<< HEAD
# Bp_estimation_docker
=======
# PPG Vitals Estimation Pipeline

Real-time cuffless vitals estimation for continuous patient monitoring.
Consumes PPG data from Kafka, runs AI analysis, writes results to MongoDB.

---

## What This System Does

```
Berry Watch / PPG Device → Kafka Topic → [This Container] → MongoDB
```

For every 30-second PPG recording received:
1. Extracts PPG signal (PlethWave[32:] for NISO204 devices, or Pleth for Berry watch)
2. Resamples to 120 Hz and filters signal
3. Detects cardiac cycles, extracts pulse morphology features
4. Runs BP classifier (Hypo / Normal / Hyper) + per-group SBP/DBP regressors
5. Runs Hb and Glucose regressors
6. Applies personal calibration offsets (carry-forward from cuff reference)
7. Writes structured result to MongoDB with processingStatus: null

---

## AI Models

| Vital | Method | Notes |
|-------|--------|-------|
| SBP / DBP | XGBoost hierarchical (hypo/normal/hyper groups) | Trained on 120 Hz PPG |
| Hemoglobin | XGBoost regressor | Pulse morphology features |
| Glucose | XGBoost regressor | Pulse morphology features |

Models are baked into the Docker image (`modelsss/` directory).

---

## Setup

### 1. Environment Variables

Copy `.env.template` to `.env` and fill in your values:

```bash
cp .env.template .env
```

Required variables:

```
KAFKA_BOOTSTRAP_SERVERS=your-kafka-broker:9092
KAFKA_TOPIC=ppg-vitals-topic
KAFKA_GROUP_ID=ppg-vitals-consumer-group
MONGO_URI=mongodb://user:password@your-mongo-host:27017
MONGO_DB=vitals_db
MONGO_COLLECTION=ppg_vitals_results
FACILITY_ID=CF0000000000 (can be cahnges based on the facility)
```

### 2. Build Docker Image

```bash
docker build -t lifesigns-vitals .
```

Build takes 3–5 minutes (installs scipy, xgboost, scikit-learn).

### 3. Run

```bash
docker run --env-file .env lifesigns-vitals
```

---

## Kafka Message Format (Input)

```json
{
  "admissionId":   "ADM819104078",
  "patientId":     "MRN54642783626",
  "facilityId":    "CF1315821527",
  "deviceId":      "BM-001",
  "timestamp":     1770912322923,
  "Age":           45,
  "Gender":        "Male",
  "BMI":           25.9,
  "PlethWave":     [... 3632 values, first 32 are header, real signal starts at index 32 ...],
  "PRAllData":     [72, 73, 71, ...],
  "Reference_SBP": null,
  "Reference_DBP": null,
  "offsets":       {}
}
```

**Signal field options (one of):**
- `PlethWave` — NISO204 format: first 32 bytes = packed ADC header, signal starts at index 32, always 120 Hz
- `Pleth` — Berry Med watch format: raw PPG bytes at 100 Hz

**Minimum signal length:** 25 seconds (3000 samples at 100 Hz, or 3000 at 120 Hz)

---

## MongoDB Document Format (Output)

```json
{
  "uuid":        "b3f1c2d4-...",
  "admissionId": "ADM819104078",
  "patientId":   "MRN54642783626",
  "facilityId":  "CF1315821527",
  "deviceId":    "BM-001",
  "timestamp":   1770912322923,

  "input": {
    "age": 45,
    "gender": "Male",
    "bmi": 25.9,
    "source_hz": 120,
    "input_samples": 3568,
    "signal_source": "PlethWave[32:]",
    "calibration_used": false
  },

  "vitals": {
    "sbp": 128.4,
    "dbp": 79.1,
    "bp_category": "normal",
    "hb": 13.2,
    "glucose": 98.5
  },

  "offsets": {},
  "processingStatus": null,
  "processedAt":      null,
  "processedBy":      null,
  "_processing_time_s": 1.8,
  "_processed_utc":     "2025-04-27T10:32:00+00:00"
}
```

**`processingStatus`** is written as `null` — the UI/clinical team sets it to `"processed"` or `"reviewed"` after sign-off.

**`bp_category`** values: `"hypo"`, `"normal"`, `"elevated"`, `"hyper_stage1"`, `"hyper_stage2"`

---

## File Structure

```
├── vitals_kafka_consumer.py    Entry point — Kafka consumer loop (3 worker threads)
├── vitals_processor.py         Pipeline: extract signal → inference → build MongoDB doc
├── vitals_mongo_writer.py      MongoDB write with retry
├── vitals_standalone.py        Core inference wrapper
├── inference_engine.py         XGBoost model loading + prediction
├── kafka_config.py             All config (reads from env vars)
├── config.py                   Signal processing + model path constants
├── Dockerfile
├── requirements.txt
├── .env.template
└── modelsss/
    ├── classifier.pkl              BP group classifier (hypo/normal/hyper)
    ├── global_feature_scaler.pkl   Feature scaler
    ├── hypo_models.pkl             Hypo group SBP/DBP regressors
    ├── normal_models.pkl           Normal group SBP/DBP regressors
    ├── hyper_models.pkl            Hyper group SBP/DBP regressors
    ├── hb_regressor.pkl            Hemoglobin regressor
    ├── scaler_hb.pkl
    ├── glucose_regressor.pkl       Glucose regressor
    └── scaler_glucose.pkl
```

---

## Requirements

- Docker (no other local dependencies needed)
- Kafka broker accessible from container
- MongoDB instance accessible from container
- Python 3.13 (handled by Docker)
>>>>>>> 11dcfa3 (Intial Commit)
