# Production setup

This repository supplies an integration service and a deployment baseline. It does not provision your identity provider, Temporal cluster, adapters, certificates, ingress, or backup system. Keep the local demo namespace separate from production.

## Prepare the environment

1. Use Temporal Cloud or an operated self-hosted Temporal Service with persistent storage, tested backups, TLS, and access control. Create the Temporal namespace and set its retention. Do not use `k8s/temporal-dev.yaml` in production.
2. Configure an HTTPS JWT issuer and this API's audience. Give clients `workflow.write` to submit/cancel and `workflow.read` to view. Give monitoring a separate token with `workflow.metrics`.
3. Implement the [adapter contract](INTEGRATION.md). Test a completed write followed by a lost reply, repeated calls, reverse undo, and undo racing an in-flight write. Do not go live if the provider cannot enforce these rules.
4. Build, scan, push, and approve a Java 25 image. Pin its registry digest. For repeatable builds, also pass approved digest references as Docker `BUILD_IMAGE` and `RUN_IMAGE` arguments. Existing Kotlin/Spring/Temporal versions were kept; review their compatibility and security findings in your release process.
5. Choose a Kubernetes namespace, an HTTPS ingress, and a network plugin that enforces NetworkPolicy. The template creates a ClusterIP service only; your platform must expose it through TLS ingress or an authenticated private gateway.

## Supply configuration

Use your secret manager or existing deployment process to create these objects in the chosen namespace. No credentials are committed or created by the template.

| Object | Keys | Read by |
|---|---|---|
| Secret `temporal-client` | `tls.crt`, `tls.key` | API and workers |
| Secret `workflow-api` | `cursor-signing-key` (at least 32 random bytes) | API only |
| ConfigMap `workflow-registry` | `integration.properties` from the registry example, with your HTTPS URLs and rules | API and workers |
| Secret `workflow-adapter-auth` | `tokens.properties` from the token example, with real provider tokens | Workers only |

For a private Temporal CA, add `ca.crt` to `temporal-client` and set `TEMPORAL_TLS_CA_FILE=/etc/temporal/ca.crt` on both Deployments. Public CAs use the JVM trust store by default. Certificates must be accepted by the configured Temporal endpoint and namespace.

A shared cursor key lets all API replicas read the same event cursor. Rotate it deliberately: old cursors become invalid; clients can restart event paging. Tokens and TLS contexts are loaded at startup, so rotate certificates/provider tokens with a safe rolling restart. Remove old secrets after the new replicas are healthy.

```bash
export APP_NAMESPACE=workflow-prod
export APP_IMAGE='your-registry/workflow@sha256:YOUR_APPROVED_64_HEX_DIGEST'
export TEMPORAL_TARGET='your-temporal-host:7233'
export TEMPORAL_NAMESPACE='your-temporal-namespace'
export AUTH_ISSUER_URI='https://your-identity-provider/issuer'
export AUTH_AUDIENCE='workflow-api'
export INGRESS_NAMESPACE=ingress-nginx
export MONITORING_NAMESPACE=monitoring

# Render and check input only. It changes no cluster resources.
./scripts/check-prod.sh /tmp/workflow-prod.yaml --offline

# After your platform creates the namespace and configuration objects:
# check required keys and lengths, then validate with the API server without applying.
./scripts/check-prod.sh /tmp/workflow-prod.yaml --server
```

The example image placeholder is deliberately rejected; supply your approved image digest and real endpoint values. The server check verifies required configuration keys, nonempty certificate/key and registry values, and a UTF-8 cursor key of at least 32 bytes. The worker token file key must exist, but its file may be empty when every adapter is public. It does not validate certificate trust, provider credentials, or service reachability. Inspect the rendered manifest and deploy it through your normal release process. Do not point the local setup script at production.

## What the template does

- Runs two API pods and two v1 workers, with rolling updates, separate disruption budgets, and a preference to spread pods across hosts. Provision multiple hosts; the preference is not a guarantee of availability.
- Uses non-root containers, read-only filesystems, a small writable `/tmp`, explicit resources, and no Kubernetes API token.
- Keeps demo pages, pod controls, and simulated activities out of the production surface. Crash control is disabled, and the production guard rejects enabling it.
- Probes `/actuator/health/liveness` and `/actuator/health/readiness` on management port 8081. Liveness must not depend on a remote service; readiness checks whether the app can serve its work.
- Allows API ingress only from the configured ingress namespace. Monitoring port 8081 is allowed only from the configured monitoring namespace. Prometheus still needs a JWT with `workflow.metrics`; probes see no secret health details.
- Allows DNS and outbound TCP 443/7233. This baseline limits ports, not destination hosts: tighten destinations using your CNI, an egress gateway, or approved service CIDRs. Adjust ports for approved private endpoints. Never remove all policies just to fix connectivity.

If your cluster uses NodeLocal DNS, add its approved link-local DNS address to the DNS egress rule. The default rule allows DNS pods in `kube-system` and may not cover a node-local resolver. Review this with the cluster operator instead of opening all outbound traffic.

No production PostgreSQL or Temporal server is installed by this template. Their availability and recovery belong to the operated Temporal deployment.

## Release and recovery checks

Before a production release, require all of these:

- Automated tests pass, including replay, transient failures, undo failures, and cancellation.
- Saved histories from each supported previous release replay on the candidate worker. Test histories made by the current code do not prove cross-release compatibility.
- JWT issuer/audience/scopes and caller ownership tests pass through the real ingress. Invalid or missing tokens fail closed.
- Repeated job IDs and repeated provider calls do not create duplicate business writes. Test the crash-after-write-before-reply case.
- Kill a worker while it is working. Confirm the job progresses on another worker without losing recorded results. Test a dependency outage longer than the activity retry window and verify the manual-review result.
- Restore Temporal persistence from backups into a separate environment and check old jobs. A PVC alone is not a backup. Agree on data-loss and recovery-time limits.
- Measure expected load and peak load. Watch task schedule-to-start delay, activity failures/timeouts/retries, workflow failures, worker saturation, API latency, and storage growth. Configure alerts and route `NEEDS_ATTENTION` results to an operator.
- The workflow emits `job_business_result` with a bounded `outcome` label. Alert on `NEEDS_ATTENTION` and unexpected `FAILED` outcomes as well as Temporal execution failures. Confirm the exported Prometheus name in your registry before installing alert rules. These counters are approximate operational signals, not an exactly-once business ledger; reconcile with saved job results.
- Ship stdout logs and Kubernetes events to a durable store. Configure Prometheus to scrape `/actuator/prometheus` on port 8081 using its monitoring token. Browser logs are not the audit record.
- Review image/dependency findings, pin production artifacts, test credential rotation, and rehearse rollback while old workers still exist.

Use a new workflow type/queue for incompatible changes, or adopt compatible Temporal Worker Versioning. Keep old workers until their runs are drained. Never replace all old workflow code merely because a new image builds.

See Temporal's [production checklist](https://docs.temporal.io/self-hosted-guide/production-checklist), [worker guidance](https://docs.temporal.io/best-practices/worker), and Kubernetes [network policy](https://kubernetes.io/docs/concepts/services-networking/network-policies/) and [disruption budget](https://kubernetes.io/docs/tasks/run-application/configure-pdb/) guidance.

The initial V1 replay fixture is checked in under `src/test/resources/replay`. CI replays that fixed history as well as newly generated histories. It is a starting baseline, not evidence for an unknown previous production release.
