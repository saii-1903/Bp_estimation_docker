import json
import sys
import os
import argparse
import numpy as np

# Add current directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.append(script_dir)

try:
    import config as cfg
    from inference_engine import VitalInferenceEngine
except ImportError:
    # Try adding the current directory again just in case
    sys.path.append(os.getcwd())
    try:
        import config as cfg
        from inference_engine import VitalInferenceEngine
    except ImportError as e:
        print(f"Error: Could not import inference_engine or config ({e}).")
        sys.exit(1)

def _extract_pleth(json_data):
    """
    Returns (pleth_array, hz) picking the correct signal source:

    Priority order:
      1. PlethWave[32:]  — NISO204 device data (first 32 bytes are a packed
                           24-bit ADC header, real PPG starts at index 32).
                           Always 120 Hz regardless of the FS field.
      2. Pleth           — Berry Med watch data. The watch stores its
                           PlethWave (Byte 12, range 1-100) here at whatever
                           rate the host commanded (default 100 Hz).

    Both signals are in the 0-100 range so features are directly comparable,
    allowing models trained on NISO204 PlethWave to run on Berry input.
    """
    pw = json_data.get("PlethWave", [])
    if len(pw) > 32:
        # NISO204 format: skip 32-byte packed header, signal is always 120 Hz
        return list(pw[32:]), 120

    # Berry watch format: PlethWave Byte12 stored as "Pleth" by the host app
    return json_data.get("Pleth", []), None


def process_vitals(json_data, source_hz=None):
    """
    Processes the vitals from a JSON object.
    Matches the training format (3600 samples @ 120 Hz).
    Automatically slices the last 30 seconds if data has no limits.

    Signal source selection (see _extract_pleth):
      - NISO204 data  -> PlethWave[32:] at 120 Hz
      - Berry watch   -> Pleth (PlethWave Byte 12) at 100/200 Hz
    """
    engine = VitalInferenceEngine()

    # 1. Extract Signal — prefer PlethWave[32:] (NISO204), fall back to Pleth (Berry)
    pleth_full, detected_hz = _extract_pleth(json_data)
    pr_data_full = json_data.get("PRAllData", [])
    age    = json_data.get("Age", 35.0)
    gender = json_data.get("Gender", "Male")

    # BMI: use direct field if present, else compute from Height/Weight
    if json_data.get("BMI"):
        bmi = float(json_data["BMI"])
    elif json_data.get("Height") and json_data.get("Weight"):
        h_m = float(json_data["Height"]) / 100.0
        bmi = round(float(json_data["Weight"]) / (h_m ** 2), 1)
    else:
        bmi = 24.5

    # 2. Determine Sampling Rate
    # Priority: caller arg > JSON Source_HZ > detected from signal source > config default
    hz = source_hz or detected_hz or json_data.get("Source_HZ") or cfg.SAMPLING_RATE_HZ
    print(f"DEBUG: Processing stream at {hz} Hz")

    # 3. Windowing — need at least 25 seconds (5 complete 5s BP segments).
    # PlethWave[32:] from NISO204 gives 3568 samples (29.7s @ 120Hz), which is
    # just under 3600 but yields 5 valid segments — enough for a reliable result.
    min_samples = int(hz * 25)
    ideal_samples = int(hz * 30)

    if len(pleth_full) < min_samples:
        return {
            "status": "error",
            "message": f"Insufficient data. Need at least 25s ({min_samples} samples), but have {len(pleth_full)}."
        }

    # Take the last 30 seconds (or all data if slightly under 30s)
    pleth = pleth_full[-ideal_samples:] if len(pleth_full) >= ideal_samples else pleth_full
    # Heart rate data is 1Hz — take last 30 values
    pr_data = pr_data_full[-30:] if len(pr_data_full) >= 30 else pr_data_full

    print(f"DEBUG: Windowed last {len(pleth)} samples for analysis.")

    # 4. Calibration Management
    # Rule: Use 'Reference_*' to SET baseline, otherwise use stored 'offsets', otherwise use RAW.
    offsets = json_data.get("offsets", {}).copy()
    ref_s = json_data.get("Reference_SBP")
    ref_d = json_data.get("Reference_DBP")
    ref_h = json_data.get("Reference_Hb")
    ref_g = json_data.get("Reference_Glucose")

    # If new baseline reference provided (cuff reading)
    if any(r is not None for r in [ref_s, ref_d, ref_h, ref_g]):
        print("--- Calibration/Baseline inputs detected. Recalculating offsets... ---")
        baseline = engine.predict_vitals(
            ppg_segment=pleth,
            actual_rate_hz=hz,
            age=age,
            gender=gender,
            bmi=bmi,
            pr_all_data=pr_data,
            offsets=None # Clean baseline
        )
        
        if baseline:
            # Calculate and store the shifts
            if ref_s is not None: offsets["sbp"] = round(ref_s - baseline.get("sbp", 0), 2)
            if ref_d is not None: offsets["dbp"] = round(ref_d - baseline.get("dbp", 0), 2)
            if ref_h is not None: offsets["hb"] = round(ref_h - baseline.get("hb", 0), 3)
            if ref_g is not None: offsets["glucose"] = round(ref_g - baseline.get("glucose", 0), 1)
            print(f"DEBUG: New Baseline Offsets set: {offsets}")

    # 5. Final Calculation
    print("--- Running AI Models (Inference) ---")
    results = engine.predict_vitals(
        ppg_segment=pleth,
        actual_rate_hz=hz,
        age=age,
        gender=gender,
        bmi=bmi,
        pr_all_data=pr_data,
        offsets=offsets if offsets else None
    )
    
    if results:
        return {
            "status": "success",
            "sbp": results.get("sbp"),
            "dbp": results.get("dbp"),
            "category": results.get("bp_category"),
            "hb": results.get("hb"),
            "glucose": results.get("glucose"),
            "active_offsets": offsets,
            "metadata": {
                "source_hz": hz,
                "window_seconds": 30,
                "input_samples": len(pleth),
                "resampled_target": int(30 * 120)
            }
        }
    
    return {"status": "error", "message": "Inference failed (possibly poor signal quality)."}

def main():
    parser = argparse.ArgumentParser(description="Standalone Vitals Processor (30s Window Logic)")
    parser.add_argument("input_json", help="Path to input JSON")
    parser.add_argument("--hz", type=float, help="Force source sampling rate (e.g. 100)")
    parser.add_argument("--output", help="Save result to separate JSON")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_json):
        print(f"Error: {args.input_json} not found.")
        sys.exit(1)
        
    try:
        with open(args.input_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error: Could not parse JSON. {e}")
        sys.exit(1)
        
    print(f"\n--- Standalone Vitals Analysis: {args.input_json} ---")
    output = process_vitals(data, source_hz=args.hz)
    
    if output["status"] == "success":
        print("\n" + "="*30)
        print(f" BLOOD PRESSURE: {output['sbp']}/{output['dbp']} mmHg")
        print(f" CATEGORY:       {output['category']}")
        print(f" HEMOGLOBIN:     {output['hb']} g/dL")
        print(f" GLUCOSE:        {output['glucose']} mg/dL")
        print("="*30)
        
        if output["active_offsets"]:
            print(f"\nACTIVE OFFSETS: {json.dumps(output['active_offsets'])}")
            print("(Save these for following 30s chunks to maintain calibration.)")
            
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(output, f, indent=4)
            print(f"\nResults saved to: {args.output}")
    else:
        print(f"\nERROR: {output['message']}")

if __name__ == "__main__":
    main()
