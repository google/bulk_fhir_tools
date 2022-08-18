# Medical Claims Tools
<a href="https://github.com/google/medical_claims_tools/actions">
  <img src="https://github.com/google/medical_claims_tools/workflows/go_test/badge.svg" alt="GitHub Actions Build Status" />
</a>
<a href="https://godoc.org/github.com/google/medical_claims_tools">
  <img src="https://godoc.org/github.com/google/medical_claims_tools?status.svg" alt="Go Documentation" />
</a>

This repository contains a set of libraries, tools, notebooks and documentation
for working with Medical Claims data. In particular, this contains an example
program and documentation to set up [periodic FHIR claims data ingestion](docs/periodic_gcp_ingestion.md) to local
disk or GCP's
[FHIR Store](https://cloud.google.com/healthcare-api/docs/how-tos/fhir)
from [Medicare's Beneficiary Claims Data API (BCDA)](https://bcda.cms.gov/).

This repository also contains example [analysis notebooks](analytics)
using synthetic data that showcase query patterns once the data is in FHIR Store
and BigQuery.

Note: This is not an official Google product.

__If using these tools with protected health information (PHI), please be sure
to follow your organization's policies with respect to PHI.__

## Overview
<!---TODO(b/199179306): add links to code paths below.--->
* `bcda/`: A go client package for interacting with the [BCDA](https://bcda.cms.gov/).
* `cmd/bcda_fetch/`: A configurable example CLI program for fetching data from
  BCDA, and optionally saving to disk or sending to your FHIR Store. The program
  is highly configurable, and can support pulling incremental data only, among
  other features.
* `analytics/`: A folder with some analytics notebooks and examples.
* `fhirstore/`: A go helper package for uploading to FHIR store.
* `fhir/`: A go package with some helpful utilities for working with FHIR claims
  data.

## bcda_fetch CLI Usage

The example `bcda_fetch` command line program can be used to fetch data from
the BCDA to save to disk or validate and upload to a [FHIR Store](https://cloud.google.com/healthcare-api/docs/how-tos/fhir). This program can
also be configured to run as a periodic cron job where it only fetches new data
since the program last successfully ran.

### Build

To build the program from source run the following from the root of the
repository (note you must have [Go](https://go.dev/dl/) installed):

```sh
go build cmd/bcda_fetch/bcda_fetch.go
```

To build on a GCP VM, you can follow these [instructions](docs/gcp_vm_setup.md)
to get the environment setup.

### Common Usage

You can check all of the various flag details by running `./bcda_fetch --help`.
This section will detail common usage patterns for the command line tool.

If you want to try this out __without__ using your real credentials,
you can use the synthetic data sandbox credentials (client_id and
client_secret) from one of the options
[here](https://bcda.cms.gov/guide.html#try-the-api).

__If using these tools with protected health information (PHI), please be sure
to follow your organization's policies with respect to PHI.__

* __Fetch all BCDA data for your ACO to local NDJSON files:__

  ```sh
  ./bcda_fetch \
    -client_id=YOUR_CLIENT_ID \
    -client_secret=YOUR_SECRET \
    -bcda_server_url="https://sandbox.bcda.cms.gov" \
    -output_prefix="/path/to/store/output/data/prefix_" \
    -use_v2=true \
    -alsologtostderr=true -stderrthreshold=0
  ```
  Change the -bcda_server_url as needed. You will need to change it if you are
  using the production API servers.

  Feel free to change the stderrthreshold depending on what kinds of logs you
  wish to see. More details on logging flags can be found [here](https://github.com/google/glog#setting-flags).

* __Rectify the data to pass R4 Validation.__ By default, the FHIR R4 Data
returned by BCDA does not satisfy the default FHIR R4 profile at the time of
this software release. This CLI provides an option to tag the expected missing
fields that BCDA does not map with an extension (if they are indeed missing)
that will allow the data to pass R4 profile validation (and be uploaded to FHIR
store, or other R4 FHIR servers). To do this, simply pass the following flag:

  ```sh
  -rectify=true
  ```

* __Fetch all claims data _since_ some timestamp__. This is useful if, for example,
you only wish to fetch new claims data since yesterday (or some other time).
Simply pass a [FHIR instant](https://www.hl7.org/fhir/datatypes.html#instant)
timestamp to the `-since` flag.

  ```sh
  -since="2021-12-09T11:00:00.123+00:00"
  ```
  Note that every time fetch is run, it will log the BCDA transaction time,
  which can be used in future runs of fetch to only get data since the last run.
  If you will be using fetch in this mode frequently, consider the since file
  option below which automates this behavior.

* __Automatically fetch new claims since last successful run.__ The program
provides a `-since_file` option, which the program uses to store and read BCDA
timestamps from successful runs. When using this option, the fetch program will
automatically read the latest timestamp from the since_file and use that to only
fetch claims since that time. When completed successfully, it will write a new
timestamp back out to that file, so that the next time fetch is run, only claims
since that time will be fetched. The first time the program is run with
`-since_file` it will fetch all historical claims from BCDA and initialize the
since_file with the first timestamp.

  ```sh
  -since_file="path/to/some/file"
  ```
Note, do not run concurrent instances of fetch that use the same since file.

* __Upload claims to a GCP FHIR Store:__

  ```sh
  ./bcda_fetch \
    -client_id=YOUR_CLIENT_ID \
    -client_secret=YOUR_SECRET \
    -bcda_server_url="https://sandbox.bcda.cms.gov" \
    -output_prefix="/path/to/store/output/data/prefix_" \
    -use_v2=true \
    -rectify=true \
    -enable_fhir_store=true \
    -fhir_store_gcp_project="your_project" \
    -fhir_store_gcp_location="us-east4" \
    -fhir_store_gcp_dataset_id="your_gcp_dataset_id" \
    -fhir_store_id="your_fhir_store_id" \
    -alsologtostderr=true -stderrthreshold=0
  ```

  Note: If `-enable_fhir_store=true` specifying `-output_prefix` is optional. If
  `-output_prefix` is not specified, no NDJSON output will be written to local
  disk and the only output will be to FHIR store.

To set up the `bcda_fetch` program to run periodically, take a look at the
[documentation](docs/periodic_gcp_ingestion.md).


