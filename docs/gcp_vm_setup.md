# GCP VM Setup for medical_claims_tools

This documentation details how to setup a standard GCP Debian VM to build and
run `bcda_fetch` from source. It also details how to configure the VM service
account to have the ability to upload to FHIR store.

1.  Create a
    [new linux virtual machine](https://cloud.google.com/compute/docs/instances/create-start-instance).
    To ensure `bcda_fetch` running on this VM has permission to upload to FHIR
    store:
    *   While creating the virtual machine, you will need to ensure that the
        default service account on the VM has access to write to the FHIR store.
        This setting can be found under "Identity and API access" when creating
        a VM and you can either set access per-API for FHIR store or set "Allow
        full access to all Cloud APIs." The "Allow default access" setting will
        not work, as it sets read-only access.
    *   After creating the virtual machine, ensure the VM service account has
        "Healthcare FHIR Resource Editor" permission on the relevant datastore.
        *   Your default service account should be of the form: `PROJECT_NUMBER-compute@developer.gserviceaccount.com`.
        *   To verify that that service account has the "Healthcare FHIR Resource Editor" permission, navigate to `Google Cloud > Healthcare > Browser` and select the dataset of interest/ create one if none exists. Select the datastore of interest/ create one if none exists. The on the `PERMISSIONS` section (on the right hand side) expand the "Healthcare FHIR Resource Editor" tab and if you see your default service account then your permissions are setup properly.
        *   If you do not see your service account, you can add it by clicking on `ADD PRINCIPAL`, adding your service account into the `New principals` section and selecting "Healthcare FHIR Resource Editor" in the `Select a role` section and clicking `SAVE`.
2.  Install `git`:

    ```sh
    sudo apt-get install git
    ```

3.  If you would like to build `bcda_fetch` from source, Go must be installed:

    1.  On the [Go download website](https://go.dev/dl/) right-click on the
        "Linux" button from the featured downloads and copy the link address.
    2.  Install `wget`: `sudo apt-get install wget`
    3.  Download the Go installer to the machine: `wget <copied download link>`
    4.  Follow the instructions on the [Go website](https://go.dev/doc/install)
        to remove the previous install and unpack the newly downloaded one. The
        command will look something like `rm -rf /usr/local/go && tar -C
        /usr/local -xzf <installer name>`

4.  Clone the medical_claims_tools repository

    ```sh
    git clone https://github.com/google/medical_claims_tools
    ```

5.  Build `bcda_fetch`:

    ```sh
    go build cmd/bulk_fhir_fetch/bulk_fhir_fetch.go
    ```
