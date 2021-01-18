# BigQueryLogCollector

A CLI tool which collects BigQuery jobs from half a year back.

[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)][license]
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)][license]

BigQuery jobs could be monitored through Stackdriver Logging(*1), INFORMATION_SCHEMA, or any similar ways. However, neither of these monitoring methods don't fulfill the needs below at onece: 

1. Monitoring isn't set beforehand
1. Need job history older than the last 180 days

**BigQueryLogCollector** provide simple CLI tool which collects BigQuery job history and options to test and/or store the histories to BigQuery table for analysis use.

## Usage


### Options

| option | description | example |
| ----- | ---- | --- |
| -a, --append | Store  |
| -d, --dryrun | Display collected job histories but DONOT store them |
| -i, --infinite | Loop API call until it hits the end |
| -t, --table_id | Define table_id to store the collected data |
| -M, --maxCreationTime | Define maxCreationTime to anker the collection start point |




