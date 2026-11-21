# Temporal workflow service

A Java 25 / Kotlin backend that runs named tasks, saves progress, retries short failures, and requests safe cleanup after failure or cancellation.

## Run locally

```bash
./run-demo.sh
```

Keep the terminal open. Open [the guide](http://localhost:8080/temporal-what-is.html) or [the visual recovery page](http://localhost:8080/temporal-live-demo.html). The visual order steps are simulated and keep 20-second waits. Local mode has no login; use it only on a trusted local machine.

## Connect your software

Use the JSON API at `/api/v1/jobs`. Register your HTTP adapters on the server; callers never supply service URLs or credentials.

- [API and adapter steps](docs/INTEGRATION.md)
- [OpenAPI contract](docs/openapi.yaml)
- [Production configuration](docs/PRODUCTION.md)
- [Local database backup and migration](HELP.md)

Production uses the `prod` profile, JWT scopes and caller ownership, Temporal mTLS, configured HTTPS adapters, and an external operated Temporal service. It refuses unsafe or missing settings. It does not enable the demo or crash controls.

Your adapters must enforce duplicate protection and coordinate execute/undo calls. Real endpoints, credentials, database recovery, capacity checks, and release approval belong to your environment; a passing build cannot verify them.

## Verify changes

```bash
./gradlew --no-daemon test
bash -n run-demo.sh scripts/check-prod.sh
```

Tests cover the real API, retries, replay, cancellation, cleanup, HTTP limits, JWT rules, and caller boundaries. The checked-in V1 replay history is the initial test baseline; add representative deployed histories before upgrading live workflow code.
