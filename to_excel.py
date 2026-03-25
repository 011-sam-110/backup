"""
Convert the latest (or a specific) chargers JSON file to Excel.

Produces two sheets:
  Bays  — one row per charging bay (location) with summary counts
  EVSEs — one row per individual connector point

Usage:
    python to_excel.py                  # converts the most recent output file
    python to_excel.py output/chargers_20260323T180000Z.json
"""

import json
import sys
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path("output")


def latest_json() -> Path:
    files = sorted(OUTPUT_DIR.glob("chargers_*.json"))
    if not files:
        raise FileNotFoundError(f"No charger JSON files found in {OUTPUT_DIR}/")
    return files[-1]


def build_sheets(data: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    bay_rows = []
    evse_rows = []

    for rec in data:
        # ── Bay row ────────────────────────────────────────────────────────
        bay = {
            "uuid": rec.get("uuid"),
            "latitude": rec.get("latitude"),
            "longitude": rec.get("longitude"),
            "max_power_kw": rec.get("max_power_kw"),
            "total_devices": rec.get("total_devices"),
            "total_evses": rec.get("total_evses"),
            "connector_types": ", ".join(rec.get("connector_types") or []),
            "available_count": rec.get("available_count"),
            "charging_count": rec.get("charging_count"),
            "inoperative_count": rec.get("inoperative_count"),
            "out_of_order_count": rec.get("out_of_order_count"),
            "unknown_count": rec.get("unknown_count"),
            "scraped_at": rec.get("scraped_at"),
        }

        # Append any top-level scalar fields from location_raw that aren't
        # already covered — catches name, address, operator, etc. automatically.
        for k, v in (rec.get("location_raw") or {}).items():
            if k not in bay and not isinstance(v, (dict, list)):
                bay[f"loc_{k}"] = v

        bay_rows.append(bay)

        # ── EVSE rows ──────────────────────────────────────────────────────
        bay_uuid = rec.get("uuid")
        for device in rec.get("devices") or []:
            device_uuid = device.get("device_uuid")
            for evse in device.get("evses") or []:
                evse_rows.append({
                    "bay_uuid": bay_uuid,
                    "device_uuid": device_uuid,
                    "evse_uuid": evse.get("evse_uuid"),
                    "connectors": ", ".join(evse.get("connectors") or []),
                    "network_status": evse.get("network_status"),
                    "network_updated_at": evse.get("network_updated_at"),
                    "user_status": evse.get("user_status"),
                    "user_updated_at": evse.get("user_updated_at"),
                })

    return pd.DataFrame(bay_rows), pd.DataFrame(evse_rows)


def convert(json_path: Path) -> Path:
    data = json.loads(json_path.read_text())
    df_bays, df_evses = build_sheets(data)

    out_path = json_path.with_suffix(".xlsx")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_bays.to_excel(writer, sheet_name="Bays", index=False)
        df_evses.to_excel(writer, sheet_name="EVSEs", index=False)

    print(f"Wrote {len(df_bays)} bays, {len(df_evses)} EVSEs → {out_path}")
    return out_path


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else latest_json()
    convert(path)
