# DPP PoC test datasets

## Included datasets
- `dataset_valid_3_stage/`: minimal valid chain with 3 Excel files.
- `dataset_valid_5_stage/`: valid chain with 5 Excel files and aggregation.
- `dataset_invalid_4_stage_missing_link/`: invalid chain with a broken reference.

## How the sample files work
- Each Excel file represents one form / event stage.
- Each row inside a file is an event.
- `input_lot_ids` uses `|` when more than one lot is consumed.
- The final stage includes `dpp_candidate_id`, `product_id`, and `batch_number`.

## Expected behavior in Streamlit
- Valid datasets should produce a DPP.
- Invalid dataset should report the broken link and block DPP issuance.