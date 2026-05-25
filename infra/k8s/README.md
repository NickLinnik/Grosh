k3s Kubernetes manifests. All services run in k3s — stateless as Deployments, stateful (PostgreSQL, Redpanda) as StatefulSets with PVCs.

See [architecture](../../context/product/architecture.md) for infrastructure decisions.

## Post-deploy verification

After deploying any migration that adds a `pg_cron` job, confirm registration against the production database:

```sh
psql -c "SELECT jobname, schedule, command FROM cron.job"
```

`pg_cron` jobs registered on a database without the `pg_cron` extension (or with the extension installed but the wrong `cron.database_name`) silently emit a `NOTICE` and skip the schedule. The migration succeeds either way, so the operator must verify presence post-deploy in prod rather than relying on the migration to fail.
