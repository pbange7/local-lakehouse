You are acting as a test runner and diagnostic assistant for the local-lakehouse project. The working directory is the repo root. Execute the following steps in order and report results clearly.

## Step 1 — Detect Stack State

Run: `docker compose ps --format json`

Parse the output to determine if the `kafka` container is running and healthy. Set STACK_UP=true if kafka is healthy, STACK_UP=false otherwise.

Report: **Stack: [UP | DOWN]**

## Step 2 — Unit Tests (always run, no Docker required)

Run: `make test-unit`

- PASS if exit code 0
- FAIL if non-zero: show the full pytest output and identify which test(s) failed and why

## Step 3 — Smoke Test (only if STACK_UP=true)

If STACK_UP=false: report **SKIPPED** — start the stack with `make up`

If STACK_UP=true: run `make smoke`

- PASS or FAIL
- On FAIL: run `docker compose logs kafka --tail 50` and summarize ERROR/WARN lines

## Step 4 — Producer Verification (only if STACK_UP=true)

If STACK_UP=false: report **SKIPPED**

If STACK_UP=true: run `make verify`

- PASS or FAIL
- On FAIL: run `docker compose logs producer --tail 30` and determine if the producer is crashing, slow to start, or cannot reach Kafka. Suggest `make build-producer && make restart` if the image may be stale.

## Step 5 — Integration Tests (only if STACK_UP=true)

If STACK_UP=false: report **SKIPPED** — Kafka not available at localhost:9094

If STACK_UP=true: run `make test-integration`

- PASS or FAIL
- On FAIL: show the full pytest output. Distinguish:
  - **Connection error** (Kafka down mid-test) → suggest `make status`
  - **Assertion error** (data correctness) → show actual vs expected values

## Step 6 — Final Verdict

Print this summary:

```
╔══════════════════════════════════════════╗
║  TEST RESULTS — local-lakehouse          ║
╠══════════════╦═══════════════════════════╣
║ Unit Tests   ║ PASS / FAIL               ║
║ Smoke Test   ║ PASS / FAIL / SKIPPED     ║
║ Verify       ║ PASS / FAIL / SKIPPED     ║
║ Integration  ║ PASS / FAIL / SKIPPED     ║
╠══════════════╩═══════════════════════════╣
║ VERDICT: PASS / PASS WITH WARNINGS / FAIL║
╚══════════════════════════════════════════╝
```

Verdict rules:
- **PASS**: all applicable stages passed (skipped stages are not failures)
- **PASS WITH WARNINGS**: unit tests passed but stack was down so integration stages were skipped
- **FAIL**: any stage that ran returned a non-zero exit code

If FAIL: list the specific remediation steps for each failed stage.
