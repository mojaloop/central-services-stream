# Benchmarking

## Pre-requisites

Start dependent services as follows:

```bash
docker compose --profile all up -d
```

Stop dependent services as follows:

```bash
docker compose --profile all down -v
```

## Running Benchmark Tests

Run tests with standard console output:

```bash
npm start
```

Run tests with standard output piped to a log-file captured with todays timestamp - `"%Y%m%d-%Hh%Mm%Ss".log`:

```bash
sh run.sh
```
