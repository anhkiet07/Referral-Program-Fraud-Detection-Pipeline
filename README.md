# Referral-Program-Fraud-Detection-Pipeline

Springer Capital — Take-Home Test for Data Engineer Intern

A PySpark pipeline that profiles the referral-program source data, joins it
into a single referral-detail report, and flags referrals whose reward
looks invalid/suspicious using the business rules in the test spec.

## 1. Project layout

```
referral-fraud-pipeline/
├── README.md                    <- you are here
├── Dockerfile
├── entrypoint.sh                 <- runs profiling then the pipeline
├── requirements.txt
├── data/
│   └── raw/                      <- put the 7 source CSVs here
├── src/
│   ├── utils.py                  <- shared cleaning helpers
│   ├── profiling.py               <- Test Instruction #1: data profiling
│   └── referral_pipeline.py      <- Test Instruction #2: main script
├── output/
│   ├── profiling/                 <- profiling CSVs (generated)
│   └── report/                    <- final fraud report (generated)
└── docs/
    └── data_dictionary.xlsx       <- business-facing documentation
```

## 2. Source data expected

Place these 7 files in `data/raw/` (already included in this repo for
convenience):

| File | Table |
|---|---|
| `lead_log.csv` | lead_logs |
| `paid_transactions.csv` | paid_transactions |
| `referral_rewards.csv` | referral_rewards |
| `user_logs.csv` | user_logs |
| `user_referral_logs.csv` | user_referral_logs |
| `user_referral_statuses.csv` | user_referral_statuses |
| `user_referrals.csv` | user_referrals (the base/fact table — one row per referral) |

## 3. Running with Docker (recommended)

```bash
# 1. Build the image
docker build -t referral-fraud-pipeline .

# 2. Run it, mounting your data in and getting the report out.
#    Everything under ./output on your machine will contain the results
#    (the report is written OUTSIDE the container, as required).
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  referral-fraud-pipeline
```

After it finishes, look in:
- `output/profiling/` — one CSV per source table, plus `profile_all_tables.csv`
- `output/report/referral_fraud_report.csv` — the final 46-row report

## 4. Running locally without Docker

Requires Python 3.10+ and a JVM (PySpark needs Java 8/11/17).

```bash
pip install -r requirements.txt

python src/profiling.py --input-dir data/raw --output-dir output/profiling
python src/referral_pipeline.py --input-dir data/raw --output-dir output/report
```

## 5. What the pipeline does

1. **Load** — reads all 7 CSVs into Spark DataFrames, normalizing the
   literal text `"null"` found in the raw exports into real `NULL`s.
2. **Clean**
   - De-duplicates `user_logs` and `lead_log` (both contain repeated
     identical rows per user/lead — a naive join would fan out the data).
   - Collapses `user_referral_logs` (multiple status-update rows per
     referral) into one row per referral: was the reward ever granted,
     and if so, when.
   - Parses `reward_value` (e.g. `"10 days"`) into an integer
     `num_reward_days`.
3. **Process**
   - Joins referrer details, referee/lead details, status, reward,
     transaction, and reward-log data onto the base `user_referrals` table.
   - Computes `referral_source_category` per the spec's `CASE WHEN` logic.
   - **Time adjustment**: `referral_at` / `updated_at` / `reward_granted_at`
     are converted from UTC to the *referrer's* homeclub timezone (joined
     from `user_logs`, since `user_referrals` has no timezone of its own).
     `transaction_at` uses its own `timezone_transaction` column.
   - **String adjustment**: `INITCAP` is applied to descriptive string
     columns (status, source, transaction type, etc.) but **not** to club
     or location names, which stay in their original casing.
   - **Null handling**: descriptive text fields (names, statuses, etc.)
     are filled with explicit placeholders (`"Unknown"`, `"No Transaction"`)
     rather than left blank. Timestamp fields describing an event that
     never happened (e.g. `transaction_at` when there's no transaction)
     are intentionally left blank rather than filled with a fabricated
     date, since that would corrupt the fraud-detection logic.
4. **Fraud detection** — implements the 2 "valid" and 5 "invalid" rules
   from the spec as `is_business_logic_valid` (boolean). As a bonus, a
   `fraud_reason` column explains *why* a row was flagged invalid.
5. **Output** — a single `referral_fraud_report.csv` with 46 rows (one
   per source referral), matching the expected row count in the spec.

## 6. Key finding surfaced by this analysis

None of the referrals with status **Berhasil** (Successful) in this
dataset have a reward that was actually marked as granted in
`user_referral_logs`. Every row flagged `TRUE` in the output is valid via
the "pending/failed, no reward" rule — not the "successful, reward paid
out" rule. This may indicate a delay or a sync gap between the referral
system and the rewards ledger, and is worth a follow-up with the rewards
team. See the "Key Finding" callout in `docs/data_dictionary.xlsx` for the
business-facing version of this note.

## 7. Documentation

- `docs/data_dictionary.xlsx` — column-by-column business documentation
  (Overview, Data Dictionary, and Business Rules sheets), written for
  non-technical stakeholders.
- `output/profiling/` — null counts, distinct counts, min/max, and sample
  values for every column of every source table.

## 8. Cloud storage credentials (if extended)

This pipeline currently reads/writes local files only — no credentials
are required. If you extend it to upload the report to cloud storage
(S3 / GCS / Azure Blob):

- **Never** hardcode credentials in the script or Dockerfile.
- Pass them at runtime via environment variables
  (`docker run -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=...`)
  or mount a credentials file as a volume, and read it with the relevant
  SDK's default credential chain (e.g. `boto3` picks up
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`
  automatically).
- In production, prefer IAM roles / workload identity over static keys
  wherever the deployment target supports it.

## 9. Notes / assumptions

- `referral_details_id` is a synthetic, sequential surrogate key generated
  by the pipeline for easy row referencing — it does not exist in the
  source data.
- `referee_id` is only meaningful when `referral_source = "Lead"` (it
  matches `lead_log.lead_id` in that case); for other sources,
  `referee_name` / `referee_phone` (already present on `user_referrals`)
  are the identifying fields, per the ERD note.
- Membership expiry is checked against the transaction date
  (`referrer_membership_expired_date >= transaction_at`).
