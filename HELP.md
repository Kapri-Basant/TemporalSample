# Workflow service

Use the reusable JSON API to connect another backend. Use the local pages to learn restart and replay.

- [Integration API and adapter rules](docs/INTEGRATION.md)
- [Production configuration and release checks](docs/PRODUCTION.md)
- [Task registry example](docs/integration.properties.example) and [worker token example](docs/integration-tokens.properties.example)

Production uses JWT authentication, named HTTPS adapters, mTLS to Temporal, and a separate deployment template. Real provider contracts, identity settings, credentials, and infrastructure must be supplied and verified before go-live.

## Local restart and recovery

Java 25 is required. This local stack uses Docker Desktop Kubernetes. Business steps are simulated; no payment, stock, or shipping service is called.

```bash
./run-demo.sh
```

Keep this terminal open while using the pages. `Ctrl+C` closes local port forwards; Kubernetes pods keep running. The script reports an error if either forward stops.

Open:

- [Concepts and architecture](http://localhost:8080/temporal-what-is.html)
- [Workflow operations](http://localhost:8080/temporal-live-demo.html)
- [Temporal UI](http://localhost:8233/)

## Show recovery

1. Set a user ID and start a workflow. Each activity takes about three seconds, followed by a durable 20-second wait by default.
2. Crash the worker during an activity or wait. The process exits; Kubernetes restarts its container. The pod name may stay the same; its restart count rises.
3. Keep the page open. Saved Temporal events stay readable during worker downtime. Confirm container state and restart count rather than assuming a failed HTTP call proves a restart.
4. Open the same page in a new tab. It restores the selected user and reads saved history from Temporal.
5. Wait for all five steps and the final timer to finish. A recovery can take a task timeout and retry delay.

To replace the whole worker pod:

```bash
kubectl -n temporal-demo delete pod -l app=temporal-demo-worker
kubectl -n temporal-demo get pods -l app=temporal-demo-worker -w
```

Temporal replays workflow code using saved activity results and timers. It does not restore a Java memory snapshot. An activity whose result was not recorded can run again; real side effects need idempotency enforced by the target system.

## Logs and status

```bash
kubectl -n temporal-demo logs deploy/temporal-demo-worker -f
kubectl -n temporal-demo logs deploy/temporal-demo-worker --previous
kubectl -n temporal-demo get events --sort-by=.metadata.creationTimestamp
```

- Temporal history records accepted workflow/task/activity/timer events and survives a worker restart.
- Kubernetes reports pod/container state and restart count. Its events expire; ship them to a log store for long-term retention.
- Browser polling and crash responses are observations, not proof that work was committed.
- Idle SDK task-queue polls are not saved in workflow history. Use SDK metrics for poll counts and latency. Container logs also need central collection for permanent retention.

## Storage and production limits

Do not delete the database pod to test worker recovery. A new PVC does not copy data from an existing database container: export and restore that data before replacing an unmounted database pod. Deleting the namespace or PVC may remove saved runs.

The local stack intentionally uses simulated order activities, one worker, local credentials, and local HTTP. The reusable `/api/v1/jobs` service has a separate production profile and manifest; follow [the production guide](docs/PRODUCTION.md). Crash control exists only for the local recovery exercise.

## Start safely

Use `./run-demo.sh` for both fresh and existing local installations. It builds a unique image tag, updates both app pods, preserves an existing database without a PVC, and keeps local access running. Do not apply `k8s/temporal-dev.yaml` directly over an existing unmounted database: that replaces its pod before the data is copied.

The script targets Docker Desktop Kubernetes. A separate kind or Minikube image store needs an explicit image import and its own deployment procedure.

## Move the existing local database to its PVC

Setup preserves an existing PostgreSQL Deployment without a PVC and prints a warning. Its history stays in that container until backup/restore. A fresh installation uses the PVC.

This manual operation replaces the database pod. Read all commands first and keep the backups. It assumes this exact local stack: two Temporal databases, both owned by `temporal`. Extra databases or roles need their own backup plan.

```bash
# Stop writers, but keep the old database running.
kubectl -n temporal-demo scale deploy/temporal-demo-api deploy/temporal-demo-worker deploy/temporal --replicas=0 || exit 1
kubectl -n temporal-demo wait --for=delete pod -l 'app in (temporal-demo-api,temporal-demo-worker,temporal)' --timeout=180s || exit 1

# Keep private backups outside Git. Stop if any dump check fails.
umask 077
DB_BACKUP_DIR="$(mktemp -d "$PWD/temporal-db-backup.XXXXXX")"
for db in temporal temporal_visibility; do
  kubectl -n temporal-demo exec deploy/postgres -- pg_dump -U temporal -Fc "$db" > "$DB_BACKUP_DIR/$db.dump" || exit 1
  test -s "$DB_BACKUP_DIR/$db.dump" || exit 1
  kubectl -n temporal-demo exec -i deploy/postgres -- pg_restore --list < "$DB_BACKUP_DIR/$db.dump" > /dev/null || exit 1
done
printf 'Keep these backups: %s\n' "$DB_BACKUP_DIR"
```

Copy that directory to another safe location before continuing. These next commands replace the old database pod. If restore fails, keep apps stopped and fix the restore from those dumps.

```bash
# Select only storage and PostgreSQL, leaving Temporal stopped.
python3 - "$DB_BACKUP_DIR/storage.yaml" <<'PY'
import pathlib, re, sys
text = pathlib.Path('k8s/temporal-dev.yaml').read_text()
docs = re.split(r'(?m)^---\s*$', text)
selected = [d for d in docs if re.search(r'(?m)^kind: PersistentVolumeClaim\s*$', d) or (
    re.search(r'(?m)^kind: Deployment\s*$', d) and re.search(r'(?m)^  name: postgres\s*$', d))]
pathlib.Path(sys.argv[1]).write_text('\n---\n'.join(selected))
PY
kubectl apply -f "$DB_BACKUP_DIR/storage.yaml" || exit 1
kubectl -n temporal-demo rollout status deploy/postgres --timeout=180s || exit 1

# Restore before restarting writers.
kubectl -n temporal-demo exec -i deploy/postgres -- pg_restore -U temporal -d temporal --clean --if-exists --no-owner --exit-on-error < "$DB_BACKUP_DIR/temporal.dump" || exit 1
kubectl -n temporal-demo exec deploy/postgres -- createdb -U temporal temporal_visibility || exit 1
kubectl -n temporal-demo exec -i deploy/postgres -- pg_restore -U temporal -d temporal_visibility --no-owner --exit-on-error < "$DB_BACKUP_DIR/temporal_visibility.dump" || exit 1
kubectl -n temporal-demo scale deploy/temporal --replicas=1 || exit 1
kubectl -n temporal-demo rollout status deploy/temporal --timeout=240s || exit 1
kubectl -n temporal-demo scale deploy/temporal-demo-api deploy/temporal-demo-worker --replicas=1
```

Keep the dumps until old runs are checked in Temporal UI. Rerunning setup does not restore them.

## Code and steps

The concepts page has a short file-by-file tour and explains all five order steps and their failure behavior. Business work is simulated; stock release, refunds, and real provider idempotency are not implemented.

Set `TEMPORAL_NAMESPACE` to choose the Temporal namespace (default: `default`). It must already exist in Temporal and differs from Kubernetes namespace `temporal-demo`.
