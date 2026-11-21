# Connect another backend

The service accepts HTTP jobs and calls named HTTP adapters. The machine-readable contract is [openapi.yaml](openapi.yaml). Any language can call it. Software without HTTP needs a small adapter. The old browser order demo stays in the `local` profile; production uses `/api/v1/jobs` and `JobWorkflowV1`.

## One job

1. Your backend gets a JWT with this API's audience and `workflow.write` scope.
2. It submits a unique business `jobId` and one to twenty named steps.
3. The API checks the caller, task names, input size, and allowed undo rules. It starts a workflow on `integration-v1`.
4. The worker calls each configured adapter with a stable `Idempotency-Key`. Temporal saves its result and any wait.
5. Your backend reads job status with `workflow.read`. It can read one page of saved events or request cancellation.

```bash
curl -X POST "$WORKFLOW_URL/api/v1/jobs" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"jobId":"order-1042","steps":[
    {"task":"validate-user","input":{"userId":"user-7"}},
    {"task":"reserve-stock","input":{"orderId":"order-1042","sku":"box-1"}},
    {"task":"take-payment","input":{"orderId":"order-1042"}},
    {"task":"pack-box","input":{"orderId":"order-1042"}},
    {"task":"send-order","input":{"orderId":"order-1042"}}
  ]}'
```

A start returns `202`. Reusing the same caller/job ID returns `409`; it never starts a second retained workflow. Persist your own business order IDs beyond Temporal's history retention. On an uncertain HTTP response, look up that same job ID before retrying. One caller can create many different job IDs. Another caller cannot read or cancel those jobs.

Production waits default to zero. Set `waitSeconds` for a real business delay. The local visual demo keeps its 20-second waits.

```text
GET  /api/v1/jobs/{jobId}
GET  /api/v1/jobs/{jobId}/events?pageSize=50
POST /api/v1/jobs/{jobId}/cancel
```

Events are paged, not downloaded in full on each status check. Follow `nextPageToken` with the returned `runId`; cursors expire after 15 minutes and are tied to the caller and job. Store a shared cursor signing secret across API replicas. Read the returned business result as well as Temporal's execution status: handled failure/cancellation can have a completed Temporal execution with `FAILED`, `CANCELED`, or `NEEDS_ATTENTION` as the business result.

## Adapter contract

Register tasks with `integration.tasks.<name>` in a properties file. See [integration.properties.example](integration.properties.example). Keep URLs and rules in the `workflow-registry` ConfigMap, and tokens in the worker-only `workflow-adapter-auth` Secret. See [integration-tokens.properties.example](integration-tokens.properties.example). URLs, undo URLs, and bearer tokens are server settings, never caller input. Production requires HTTPS. Replies and requests have fixed size limits; calls time out rather than holding a worker forever.

Each execute or undo URL receives a POST:

```json
{
  "jobId": "order-1042",
  "ownerId": "server-assigned-owner",
  "index": 1,
  "task": "reserve-stock",
  "input": {"orderId": "order-1042", "sku": "box-1"},
  "idempotencyKey": "stable-hash-also-sent-as-Idempotency-Key-header",
  "previousReceipts": {"0": "check-23"}
}
```

Reply with HTTP `200` or `201` and a small JSON body only after the outside task has finished:

```json
{"receipt":"reservation-87","output":{"reservationId":"reservation-87"}}
```

HTTP `202` means work is still pending. This synchronous adapter rejects it as `TASK_ASYNC_UNSUPPORTED`, even when a receipt is present; the workflow then follows its configured cleanup or manual-review path. Other `2xx` codes are also rejected. An asynchronous service needs an adapter that waits or polls for a final result before reporting completion. Do not turn an accepted job ID into a finished-task receipt.

The receipt must be nonempty and at most 256 characters. Output is optional: at most 16 string entries, with names at most 80 characters, values at most 256 characters, and at most 8 KiB of JSON in total. The client caps replies at 64 KiB and requests at 32 KiB. A call has a ten-second timeout. Network errors, HTTP 408/425/429, and 5xx errors retry with a limit; other 4xx responses stop that activity. Redirects are not followed.

The destination must save the idempotency key and result with its business change, or enforce the key through its provider. A repeated key returns the same result without repeating the write. The key stays stable across retry attempts and workflow resets. A logged key alone does not prevent a second charge. Never put credentials, card data, or large files into job input or output: Temporal stores workflow payloads in history. Send short references to protected records instead.

## Undo, cancel, and uncertain results

Undo runs in reverse step order after failure or a cancel request. It is registered before execute: the destination may finish a write even when the reply is lost. An undo adapter must be safe when execute never happened and must coordinate with any execute call still in flight. It must save a cancellation marker or another fence so a late execute call cannot apply a write after undo succeeded. Serialize execute and undo using the same logical operation (owner, job, step index, and task), even though the execute and undo idempotency keys differ. A timed-out network request can keep running in another system; idempotency alone does not cancel it.

A read-only task can set `read-only=true`. A writing task normally needs an undo URL. A deliberately irreversible task can set `allow-irreversible=true`; touching it makes later failure or cancellation require manual review. The API derives these rules from the server registry; callers cannot disable them. Do not configure a fake undo for an irreversible action. Model approval, settlement, shipment handoff, and manual review in a provider-specific workflow if they need different business rules.

If one undo exhausts its retry limit, older undo tasks stop and the result is `NEEDS_ATTENTION`. If any touched writing task is explicitly irreversible, automatic undo is skipped entirely and the result needs manual review. This avoids refunding a charge while shipment may already be irreversible. Alert an operator and inspect both systems before retrying or refunding by hand. Cancelling requests cleanup; force-terminating a Temporal workflow bypasses workflow cleanup. It cannot promise that an external call stopped at the same instant.

## Safe releases

`JobWorkflowV1` and `integration-v1` are a stable contract. Keep the old `UserOrderWorkflow` for its recorded runs. Before any release, replay saved histories from previous versions and test provider retry/undo behavior. A new incompatible job flow needs a new workflow type and queue, with old workers kept until their runs are drained. Do not rename or remove running workflow/activity types. This queue rule is an explicit compatibility boundary; it is not automatic Temporal Worker Versioning.

The checked-in tests validate code behavior; a release must also supply representative old production histories. For wider rollout and routing controls, adopt Temporal Worker Versioning after reviewing compatibility with the SDK/server version you deploy.

Temporal explains why [activities must safely handle retries](https://docs.temporal.io/activity-definition) and how to [deploy workers safely](https://docs.temporal.io/best-practices/worker).

The initial V1 replay fixture is checked in under `src/test/resources/replay`. CI replays that fixed history as well as newly generated histories. It is a starting baseline, not evidence for an unknown previous production release.

Treat a registered task name as a versioned adapter contract. Keep its URL, undo URL, and read/write rules unchanged while jobs using it can still run, retry, reset, or undo. Use a new name such as `reserve-stock-v2` for a changed contract and keep the old registration until its jobs are drained. Credential rotation may keep the same contract, but moving an existing name to a different system can send a retry or undo to the wrong provider.
