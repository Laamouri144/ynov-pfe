import argparse
from pathlib import Path

import pandas as pd


def generate_templates(input_path: Path, output_path: Path, count: int, seed: int) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path)

    required_cols = [
        'year', 'month', 'carrier', 'carrier_name', 'airport', 'airport_name',
        'arr_flights', 'arr_del15', 'arr_delay',
        'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay',
        'carrier_ct', 'weather_ct', 'nas_ct', 'security_ct', 'late_aircraft_ct',
        'arr_cancelled', 'arr_diverted',
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = 0

    df = df.dropna(subset=['carrier', 'airport'])
    if len(df) == 0:
        raise ValueError("No valid rows after filtering (missing carrier/airport)")

    target_count = min(count, len(df))

    # Prefer broad airport coverage so downstream visuals (e.g., Power BI filters)
    # have enough variety. If possible, take at least one row per airport, then
    # fill the remaining quota with a random sample.
    airport_count = df['airport'].nunique(dropna=True)
    if airport_count > 0 and target_count >= airport_count:
        per_airport = df.groupby('airport', group_keys=False).sample(n=1, random_state=seed)
        remaining = target_count - len(per_airport)
        if remaining > 0:
            filler = df.sample(n=remaining, random_state=seed)
            sample_df = pd.concat([per_airport, filler], ignore_index=True)
        else:
            sample_df = per_airport.reset_index(drop=True)
    else:
        sample_df = df.sample(n=target_count, random_state=seed).copy()

    sample_df['template_id'] = range(1, len(sample_df) + 1)

    numeric_cols = [
        'arr_flights', 'arr_del15', 'arr_delay',
        'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay',
        'carrier_ct', 'weather_ct', 'nas_ct', 'security_ct', 'late_aircraft_ct',
        'arr_cancelled', 'arr_diverted',
    ]
    for col in numeric_cols:
        sample_df[col] = pd.to_numeric(sample_df[col], errors='coerce').fillna(0)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    sample_df.to_csv(output_path, index=False)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(
        description="Generate a NiFi template CSV by sampling from the historical dataset"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=repo_root / "Airline_Delay_Cause.csv",
        help="Source CSV (default: Airline_Delay_Cause.csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=repo_root / "data" / "Nifi_Templates_1500.csv",
        help="Output template CSV (default: data/Nifi_Templates_1500.csv)",
    )
    parser.add_argument("--count", type=int, default=2000, help="Number of template rows to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    generate_templates(args.input, args.output, args.count, args.seed)
    try:
        written_rows = sum(1 for _ in args.output.open('r', encoding='utf-8')) - 1
    except Exception:
        written_rows = None
    if written_rows is None:
        print(f"Wrote templates to: {args.output}")
    else:
        print(f"Wrote {written_rows} template rows to: {args.output}")


if __name__ == "__main__":
    main()
