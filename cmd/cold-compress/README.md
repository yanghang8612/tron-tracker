This one-shot worker migrates the four exact daily table families `transactions`,
`user_token_stats`, `from_stats`, and `to_stats`, with a fixed initial plan.
It processes only finalized UTC dates strictly older than 30 days and protects
all four persisted tracking/statistics cursors. Running without `-execute` only
writes the plan. Run `-self-test` in a fresh work directory before production use;
it exercises a complete migration, cross-schema rename under WRITE locks,
post-drop checkpoint recovery, and rejection of a changed source in disposable
private schemas. Configuration credentials are read from the existing tracker
TOML and are never written to reports.

The worker copies primary-key ranges, builds identical secondary indexes while
compressing the private copy, compares every field via ordered SHA-256 and checks
column/index definitions. The final source hash and atomic rename hold WRITE
locks only on the closed daily source and its private copy; historical readers
of that table can briefly wait. Current-day ingestion is not locked. A new full
hash and schema check must pass on the promoted table before the original backup
is dropped. Any mismatch stops the job and retains the relevant copies.

Only one worker can hold the MySQL advisory lock. Health is sampled every ten
seconds. New work requires three samples with tracker lag at most 15 seconds;
two samples above 30 seconds or HTTP failures cancel the current operation.
Owned server-side DDL is explicitly interrupted with KILL QUERY; no other
processes or SQL connections are targeted.
UTC 23:00–03:00 and unfinished daily statistics pause work. Less than 64 GiB free
also pauses work. Per-table reports are synced before atomic writes/rename and
make interrupted copy, DDL, cutover and backup cleanup resumable. SQL lock waits
are limited to five seconds. `-max-tables=4` provides the initial verified batch;
restart with zero (the default) to process the remaining fixed plan.

Progress lives in `summary.json`, `health.json`, `plan.json`, and `tables/*.json`
under `-work-dir`. Live summary, health and current-table reports are also
published to the private schema’s `_job_state` table for read-only inspection. The original pilot copies of June 1 are rehashed and reused
when available. Subsequent dates run newest first to prioritize retained data;
tables already removed by normal annual retention are reported as expired.
Use `systemctl stop tracker-cold-compress` to stop the deployed worker and
`systemctl start tracker-cold-compress` to resume its persisted checkpoints.
A mismatch requires review; do not discard its retained backup or overwrite the
job reports. The MySQL temporary directory must be on the data filesystem but
outside MySQL's datadir.
