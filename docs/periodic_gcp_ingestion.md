# Periodic GCP Ingestion

This doc will detail a way to set up automatic periodic ingestion of FHIR data
using `bulk_fhir_fetch` into GCP's FHIR Store and BigQuery. Once the data is flowing
into BigQuery automatically, dashboards and notebooks can be used to explore
and analyze the data.

## Set up FHIR Store

First, a valid R4 FHIR store needs to be created. Follow the instructions
[here](https://cloud.google.com/healthcare-api/docs/how-tos/fhir) to create
an R4 FHIR Store. When configuring the FHIR store be sure to make the following
selections:

* Ensure it is an R4 FHIR Store.
* Enable "Allow update create"
* Ensure referential integrity is off.
* If you want to analyze the data in BigQuery, click "Stream resource changes
to BigQuery" and click "Add new streaming config". This will ensure whenever new
FHIR data is added to FHIR store, it will be reflected in BigQuery as well.

See the [analytics](../analytics) notebooks for examples of querying the FHIR
data in BigQuery.

## Set up `bulk_fhir_fetch` cron

Next, we will set up a recurring cron job to run the `bulk_fhir_fetch` program
with a `since_file` so that it will periodically fetch new FHIR data. The first
time it will run, it will fetch all historical data.

There are many ways to orchestrate periodic programs, so feel free to use the
method of your preference. Here, we will provide an example of using`bulk_fhir_fetch`
as a simple cron job on a GCP VM. Most VMs in GCP should have application
[default credentials provisioned for a service account](https://cloud.google.com/docs/authentication/production#automatically), and you
just need to ensure that service account has read/write permissions to your
FHIR store.


1. Follow the [GCP VM Setup](gcp_vm_setup.md) for this codebase.
2. Follow the Build instructions to build the `bulk_fhir_fetch` program from source.
3. Cron should be installed already, but activate it using
  `sudo systemctl enable cron`
4. Edit the crontab configuration by typing `crontab -e`. Here you will specify
how frequently you'd like `bulk_fhir_fetch` to run, and the `bulk_fhir_fetch` command to run.

For example, if you'd like `bulk_fhir_fetch` to run every day at 4AM and output
data to local disk only (and only fetch new data on each run, making use of the
`- since_file` flag), you can add a line like:

```
0 4 * * * ./path/to/bulk_fhir_fetch -client_id=<YOUR_CLIENT_ID> -client_secret=<YOUR_CLIENT_SECRET> -enable_generalized_bulk_import=true -fhir_server_base_url=<FHIR_SERVER_URL> -fhir_auth_url=<FHIR_SERVER_AUTH_URL> -output_dir=<PATH_TO_LOCAL_STORE> -since_file=<PATH_TO_SINCE_FILE>
```

[Read more here](https://en.wikipedia.org/wiki/Cron#Overview) to learn about
cron configurations. Saving the crontab file and exiting it should be sufficient
to install this new job and register it to be run at the next interval. Note
that the whole command for the cron configuration must be on one line.

To upload to FHIR store, pass the GCP flags as described in the [README](../README.md#bulk_fhir_fetch-configuration-examples).

