# Sample data

A small committed dataset (50 customers) the pipeline reads from. This is
the **only** source-data folder — the pipeline doesn't generate new data at
runtime, it just reads this.

## Contents

| Path | Source | Format |
|---|---|---|
| `profile/profile.db` | Internal customer profile | SQLite (3 tables: users, applications, instalments) |
| `aecb/*.xml` | AECB credit bureau | SOAP envelope + CRIF NAE response |
| `fraud/scores.json` | Fraud provider | JSON |
| `aml/callbacks.json` | AML / PEP screening | JSON |
| `_ground_truth.json` | Test only | Real customer identities used for validation |

## Regenerate (optional)

Same seed produces deterministic output:

```bash
python -m src.data_generation.generate --customers 50 --output data/sample
```

To test with more volume, pass `--customers 500` (or any number).
