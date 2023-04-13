# Performance

The [FHIR Bulk Data APIs](https://hl7.org/fhir/uv/bulkdata/) spec is designed for transferring large amounts of data for non-real-time uses cases (ex analytics). How long it takes to ingest data depends largely on the Bulk FHIR Server that the `bulk_fhir_fetch` ingestion tool is connected to. There are a couple performance bottlenecks:

1. Initial Bulk Data Kick-off Request \
After the initial Bulk Data Export request the Bulk FHIR Server begins internal export processing to prepare the requested data. This can take a long time, and is dependent on the server implementation and amount of data to export. Once prepared the Bulk FHIR Server returns a list of URLs to download the FHIR ndjson from. By default the `bulk_fhir_fetch` tool times out after 6 hours. The `bulk_fhir_fetch` tool logs "The Bulk FHIR server took %s to return URLs after the initial Bulk Data Kick-off Request.".

2. Bulk Data Output File Request \
The Bulk FHIR Server returns a list of URLs with FHIR ndjson. `bulk_fhir_fetch` tool downloads the data from each URL one at a time. If needed in the future we can improve this to download from URLs concurrently. The `bulk_fhir_fetch` tool does minimal processing and all outputs (to FHIR Store, GCS, etc) are implemented as non-blocking and concurrent. The process-url-time metric reports in minutes the time to download, process and output the FHIR data for a particular URL. The total time for all the ndjson URLs is logged "It took %s to download, process and output the FHIR from all the ndjson URLs.".

If the Bulk FHIR Server is not meeting your performance needs you can try limiting the data that is returned by the server. Depending on what the Bulk FHIR Server supports you can try since timestamps to ingest incremental data, a FHIR Group ID using the group_id flag and if you only need certain FHIR resource types you can filter using the fhir_resource_types flag. Configuration details can be found in the [README](/README.md#bulk_fhir_fetch-configuration-examples).

If the `bulk_fhir_fetch` ingestion tool is not meeting your performance needs please file a bug or complete our [survey](https://docs.google.com/forms/d/e/1FAIpQLSdmWHaGc41gWiobMT6kNd0PGPPeWGeS-LyG6CrGZ79moaUIEQ/viewform)! There are a couple low hanging performance improvements we can make if there is a need.