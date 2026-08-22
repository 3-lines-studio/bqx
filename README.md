# bqx

Read-only BigQuery tool for AX.

## Install

```sh
curl -fsSL https://ax.3lines.studio/install.sh | sh -s -- bqx
```

## Configure

```sh
export BQ_PROJECT_ID=my-project
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export AX_TOOLS=bqx
```

Credentials may also be passed directly with `BQ_CREDENTIALS_JSON`. The legacy Alfred names `ALFRED_BQ_PROJECT_ID` and `ALFRED_BQ_CREDENTIALS_JSON` remain supported during migration.

## Protocol

```sh
bqx ax-tools
printf '{"sql":"SELECT 1"}' | bqx ax-run bigquery_query
```

Bqx performs a BigQuery dry run and only executes statements BigQuery identifies as `SELECT`. Results stop at 1,000 rows.

## Google Cloud Storage

Copy a private object with the same Google credentials:

```sh
bqx gcs-copy BUCKET OBJECT FILE
```

The destination is replaced atomically only after a non-empty object is downloaded.

## Test

```sh
go test ./...
```
