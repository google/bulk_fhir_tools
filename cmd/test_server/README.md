# FHIR Bulk Data Test Server
`test_server` is a HTTP Server which serves (part of) the [FHIR Bulk Data APIs](https://hl7.org/fhir/uv/bulkdata/) spec. It is geared towards load testing or reproducible integration testing with a sequence of expected requests returning fixed data, rather than supporting arbitrary requests to simulate the behaviour of a real server.

## Test Data Folder Structure
The server runs based on NDJSON files under the directory given in the --data_dir flag, which should be organised as follows:

```
{--data_dir}/{export_group_id}/{timestamp}/{resource_type}_{index}.ndjson
```

Timestamp should be of the form YYYYMMDDTHHMMSSZ (e.g. 20230217T142649Z). This compact form avoids special characters which cannot be used in file paths on Windows. For the provided export_group_id, the server will provide the dataset with the earliest timestamp which is greater than the _since parameter (or the earliest timestamp overall if the _since parameter is unset).

If no dataset exists with a timestamp greater than the _since parameter, this server will return a 404 error to the initial $export call - this is assumed to be an error in setting up the test. If you wish to test the case of there being no changes to the data, or no data at all, you should add a timestamp folder which is empty.

If --data_dir flag is not provided the synthetic dataset in the synthetic_testdata folder will be used. The FHIR Patient resource changes names from OldFamilyName, OldGivenName to NewFamilyName, NewGivenName between timestamps.