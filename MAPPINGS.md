# MAPPINGS.md — Mapping templates for imports (optional helper)

This project supports flexible imports by letting you map broker export columns to a canonical schema.

## 1) When to use a mapping file
- Your broker’s exported CSV column names don’t match the app’s defaults.
- You want a repeatable import process (no manual remapping each time).

## 2) Broker Tax Export mapping
Files:
- `broker_tax_export_mapping.schema.json` — JSON Schema for validation
- `broker_tax_export_mapping.example.json` — example mapping you can copy/edit

### How it works
- The importer reads your CSV header row.
- It applies the `"columns"` mapping:
  - input column name -> canonical `"field"` with `"type"` parser
- Parsed rows become `broker_tax_rows` which are used for year-end reconciliation.

### Tips
- Start by importing the broker CSV once, then copy the detected headers into the mapping file.
- If your broker uses separate fields for ST/LT, map them to `term` and normalize to ST/LT.
- If your broker omits `wash_sale_disallowed`, you can leave it unmapped.
