# `slurm_usage_aggregator`
Scripts to generate aggregate data of Slurm usage

## About

## Usage

### Getting Slurm log data

Slurm logs can be pulled using the script:

```
uv run python src/GetSlurmLogs.py -c cluster_config.json 
```

For a specific cluster config file, which will specify basic information, like the
output location and keep track of when it was last run to prevent duplicating outputs.
The script will pull Slurm data from `sacct` starting at the last `end_time` in the config to approximately the current time.

### Ingesting Slurm log data

Add job data to the specified data frame with something like:

```
uv run python src/SlurmAggregate.py --ingest_cluster ClusterName -i staged_slurm_logs/clustername_2026-10-23T23\:57\:35.log  -A --silent -d output_df.feather -m metrics.json 
```

**Note:** there are no protections against ingesting duplicate data. Running the ingestion twice on the same file (or overlapping output logs) will result in double counting.

### Querying Data

Some basic data queries can be made on the data frame, e.g.:

```
uv run python src/SlurmAggregate.py  -A -d output_df.feather -m metrics.json --aggregate_accounts --aggregate_users -S 2026-01-01 -e 2026-10-28
```

and would produce an output like:

```
Cluster              QOS    CPU Hours   Memory Hours   GPU Hours   V100 GPU Hours   A100 GPU Hours   Wait Time
==============================================================================================================
M3                normal   350417.316    2455863.368     913.454          913.454              0.0    9141.677
M3          qos_priority    70128.853     273940.833         0.0              0.0              0.0       0.008
```

## Example usage

This data frame this generates can be used in ColdFront, for example, see https://github.com/SouthernMethodistUniversity/cf_plugin_demo