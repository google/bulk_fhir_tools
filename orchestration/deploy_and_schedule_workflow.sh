# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

# This script will deploy a workflow to run bulk_fhir_fetch along with a
# cloud scheduler that will trigger it periodically with the variables listed
# below. You must update the variables below to match your configuration.
#
# Once this script is run to install the workflow and scheduler with some
# set of variables, you can always trigger a one-off run of the scheduled
# workflow using the below command or via the GCP UI.
# gcloud scheduler jobs run ${SCHEDULER_NAME} --location=${LOCATION}

# ============================================================================ #
#                            Configurable Variables                            #
# ============================================================================ #
# Please update each of the below variables to configure the scheduled workflow
# run.

# Workflow and scheduler config
WORKFLOW_NAME="bulk-fetch-custom"
SCHEDULER_NAME="bulk-fetch-custom-scheduler"
SCHEDULE_CRON='0 22 * * 1-5'
# This is the service account email to be used to generate an OAuth token sent
# by the scheduler to the batch jobs service when triggering batch jobs. It
# is also used as the service account the batch job runs as. The
# caller of this script must have the iam.serviceAccounts.actAs (see
# https://cloud.google.com/iam/docs/service-accounts#user-role) for this
# service account.
# By default you can use the default compute engine service account, which looks
# like $PROJECT_NUMERIC_ID-compute@developer.gserviceaccount.com but you may
# wish to create a dedicated service account for this job as well.
# This service account will also need permission to write to the FHIR Store,
# read the Client ID and Client Secret GCP Secrets, and possibly to create
# and read GCP buckets if USE_INCREMENTAL_INGESTION=true below.
# Please update this value to the service account you would like to use.
SERVICE_ACCT="TODO_REPLACE@developer.gserviceaccount.com"

# FHIR Ingestion Flags
FHIR_SERVER_BASE_URL="https://sandbox.bcda.cms.gov/api/v2"
FHIR_AUTH_URL="https://sandbox.bcda.cms.gov/auth/token"
FHIR_AUTH_SCOPES=""  # Comma separated list, if needed.
FHIR_GROUP_ID="" # Leave empty if you don't have a group, and want to try full data export. Some servers require a group, though.
# If USE_INCREMENTAL_INGESTION="true" then a since file stored in GCS will be
# used to attempt to fetch only new data since the last successful import. Set
# this variable to "false" to turn off this behavior and fetch the full
# dataset on each run.
USE_INCREMENTAL_INGESTION="true"

# GCP Flags
PROJECT="YOUR_PROJECT"
LOCATION="us-east4"
FHIR_DATASET_ID="YOUR_DATASET_ID"
FHIR_STORE_ID="YOUR_FHIR_STORE_ID"

# The below variables are the GCP secret manager names for the FHIR API
# CLIENT_ID and CLIENT_SECRET. These are not the secret values themselves
# (which should be added to the secret manager directly). The secret names look
# something like:
# projects/$PROJECT_NUMERIC_ID/secrets/BULK_FHIR_FETCH_CLIENT_ID/versions/1
CLIENT_ID_GCP_SECRET_ID="projects/PROJECT_ID/secrets/CLIENT_ID_SECRET_NAME/versions/1"
CLIENT_SECRET_GCP_SECRET_ID="projoects/PROJECT_ID/secrets/CLIENT_SECRET_SECRET_NAME/versions/1"

# Static IP Flags
# Only set these IF you need your import job to run with a static IP address,
# for example with Medicare's BCDA in production (but not needed in sandbox).

# Set this to the numeric static external IP address e.g. 10.100.100.10 that
# you must reserve here:
# https://console.cloud.google.com/networking/addresses/list.
# Only set this if you need a static IP for the FHIR server you are interacting
# with. Most servers don't require this, but some do like Medicare's BCDA in
# production (but not for the sandbox).
STATIC_IP=""
# Set this to a name that you would like for your instance template.
# Note: This MUST be an empty string if you don't want to use a static IP
# instance template.
STATIC_IP_INSTANCE_TEMPLATE_NAME=""

# ============================================================================ #
#                               End of Variables                               #
# ============================================================================ #

# GCloud Commands:

# Setup the instance template if needed
if [ "${STATIC_IP}" != "" ]; then
  echo "Setting up Instance Template with Static IP."
  echo "If this command fails you may need to delete the instance template or rename it:"
  echo "gcloud compute instance-template delete ${STATIC_IP_INSTANCE_TEMPLATE_NAME}"

  gcloud compute instance-templates create ${STATIC_IP_INSTANCE_TEMPLATE_NAME} \
--network-interface="address=${STATIC_IP}"
fi

# Deploy the workflow:
gcloud workflows deploy ${WORKFLOW_NAME} --source="bulk_fetch_workflow.yaml" --location=${LOCATION}

# Setup a scheduler to trigger the workflow based on the $SCHEDULE_CRON
# variable. You can force a one-off run/trigger of this job (once it is created)
# at any time using the below. Useful for testing.
# gcloud scheduler jobs run ${SCHEDULER_NAME} --location=${LOCATION}
#
# After running this script for the first time, you must delete the old
# scheduler to replace it with a new one. To do so, uncomment the line below or
# delete the scheduler in the GCP UI.
# gcloud scheduler jobs delete ${SCHEDULER_NAME} --location=${LOCATION}
echo "Creating scheduler."
gcloud scheduler jobs update http ${SCHEDULER_NAME} \
    --schedule="${SCHEDULE_CRON}" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT}/locations/${LOCATION}/workflows/${WORKFLOW_NAME}/executions" \
    --time-zone="America/Los_Angeles" \
    --oauth-service-account-email=${SERVICE_ACCT} \
    --location=${LOCATION} \
    --message-body="{\"argument\": \"{
  \\\"fhir_server_base_url\\\": \\\"${FHIR_SERVER_BASE_URL}\\\",
  \\\"fhir_auth_url\\\": \\\"${FHIR_AUTH_URL}\\\",
  \\\"fhir_auth_scopes\\\": \\\"${FHIR_AUTH_SCOPES}\\\",
  \\\"location\\\": \\\"${LOCATION}\\\",
  \\\"fhir_dataset_id\\\": \\\"${FHIR_DATASET_ID}\\\",
  \\\"fhir_store_id\\\": \\\"${FHIR_STORE_ID}\\\",
  \\\"client_id_gcp_secret_id\\\": \\\"${CLIENT_ID_GCP_SECRET_ID}\\\",
  \\\"client_secret_gcp_secret_id\\\": \\\"${CLIENT_SECRET_GCP_SECRET_ID}\\\",
  \\\"use_incremental_ingestion\\\": \\\"${USE_INCREMENTAL_INGESTION}\\\",
  \\\"service_account\\\": \\\"${SERVICE_ACCT}\\\",
  \\\"instance_template_name\\\": \\\"${STATIC_IP_INSTANCE_TEMPLATE_NAME}\\\",
  \\\"fhir_group_id\\\": \\\"${FHIR_GROUP_ID}\\\"
}\"}"

echo "Done."
