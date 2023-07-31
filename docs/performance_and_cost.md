# Performance and Cost

The [FHIR Bulk Data APIs](https://hl7.org/fhir/uv/bulkdata/) spec is designed for transferring large amounts of data for non-real-time uses cases (ex analytics). How long it takes to ingest data depends largely on the Bulk FHIR Server that the `bulk_fhir_fetch` ingestion tool is connected to. There are a two performance bottlenecks:

1. **Initial Bulk Data Kick-off Request** \
After the initial Bulk Data Export request the Bulk FHIR Server begins internal export processing to prepare the requested data. This can take a long time, and is dependent on the server implementation and amount of data to export. Once prepared the Bulk FHIR Server returns a list of URLs to download the FHIR ndjson from. By default the `bulk_fhir_fetch` tool times out after 6 hours. The `bulk_fhir_fetch` tool logs "The Bulk FHIR server took %s to return URLs after the initial Bulk Data Kick-off Request.". <br> <br>
If the Bulk FHIR Server is not meeting your performance needs you can try limiting the data that is returned by the server. Depending on what the Bulk FHIR Server supports you can try since timestamps to ingest incremental data, a FHIR Group ID using the `-group_id` flag and if you only need certain FHIR resource types you can filter using the `-fhir_resource_types` flag. Configuration details can be found in the [README](/README.md#bulk_fhir_fetch-configuration-examples).

2. **Bulk Data Output File Request** \
The Bulk FHIR Server returns a list of URLs with FHIR ndjson. `bulk_fhir_fetch` tool downloads the data from each URL one at a time. If needed in the future we can improve this to download from URLs concurrently. The `bulk_fhir_fetch` tool does minimal processing and all outputs (to FHIR Store, GCS, etc) are implemented as non-blocking and concurrent. There are several metrics to figure out if `bulk_fhir_fetch` is the bottleneck at this stage. See [logs and monitoring documentation](/docs/logs_and_monitoring.md) for more details. The total time for all the ndjson URLs is logged "It took %s to download, process and output the FHIR from all the ndjson URLs.". <br> <br>

If the `bulk_fhir_fetch` ingestion tool is not meeting your performance needs please file a bug or complete our [survey](https://docs.google.com/forms/d/e/1FAIpQLSdmWHaGc41gWiobMT6kNd0PGPPeWGeS-LyG6CrGZ79moaUIEQ/viewform)! There are a couple low hanging performance improvements we can make if there is a need.

## FHIR Store Upload Options

`bulk-fhir-fetch` supports three different ways to upload data to FHIR Store; each with different performance and costs. The cost will depend mainly on the number of requests and amount of data stored. Full details at [Cloud Healthcare API pricing](https://cloud.google.com/healthcare-api/pricing).

1. **GCS Based Upload [Recommended]** \
Writes NDJSONs from the Bulk FHIR Server to [GCS](https://cloud.google.com/storage/docs), and then triggers a batch FHIR store import job from the GCS location using the [fhirStores.Import](https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import) method. The `bulk_fhir_fetch` job does NOT delete the downloaded FHIR from GCS bucket after each run. In the GCS based upload you will need to pay for the GCS storage and requests.  However, since GCS is so cheap and [fhirStores.Import](https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import) is inexpensive this upload method will likely be the cheapest of the three. To enable GCS based upload use the `-fhir_store_enable_gcs_based_upload` and `-fhir_store_gcs_based_upload_bucket` flags. GCS Based Upload is recommended for production.

2. **Individual Upload** \
Each FHIR Resource is uploaded in an individual API call to FHIR Store using the [fhir.update](https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/update) method. This will likely be the most expensive option for uploading to FHIR Store. You may also receive error code 429, "Quota exceeded for Number of FHIR operations per minute per region". Individual upload is only recommended for small tests.

3. **Batch Upload** \
Uploads batches of FHIR Resources to FHIR Store using the [fhir.executeBundle](https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/executeBundle) method. The default bundle size is 5 fhir resources, but can be overridden using the `-fhir_store_batch_upload_size` flag. To enable batch upload use the `-fhir_store_enable_batch_upload` flag. It can be tricky to find a batch size that is performant, but doesn't exceed the 50mb [fhir.executeBundle size limit](https://cloud.google.com/healthcare-api/quotas#resource_limits). For that reason GCS Based Upload is recommended for production.

## Load Tests

We ran load tests of `bulk_fhir_fetch` against the [`test_server`](/cmd/test_server/README.md) with
Synthea data and against the BCDA sandbox. The Synthea data was 23 GB and
included 20 FHIR ResourceTypes. The BCDA Sandbox data (extra large credentials)
was 20.3 GB with Patient, Coverage and ExplanationofBenefit Resource Types. The
GCS based upload method was used and rectify was set to true.

**Test Server with Synthea** \
The Bulk FHIR server took 10s to return URLs after the initial Bulk Data Kick-off Request. \
It took 1h47m to download, process and output the FHIR from all the ndjson URLs. \
Of the 1h47m it took 1h23m to download the data to GCS and 24m for the [fhirStores.Import](https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/.import).

**BCDA Sandbox (Extra Large Dataset)** \
The Bulk FHIR server took 17m to return URLs after the initial Bulk Data Kick-off Request. \
It took 5h10m to download, process and output the FHIR from all the ndjson URLs. \
Of the 5h10m it took 4h54m to download the data to GCS and 16m for the [fhirStores.Import](https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/.import).

If the `bulk_fhir_fetch` ingestion tool is not meeting your performance needs please file a bug or complete our [survey](https://docs.google.com/forms/d/e/1FAIpQLSdmWHaGc41gWiobMT6kNd0PGPPeWGeS-LyG6CrGZ79moaUIEQ/viewform).