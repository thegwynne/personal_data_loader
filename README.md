# Personal Data Loader

Infrastructure to allow the ingestion of personal data from multiple sources into bigquery.

Technologies used:

*   Cloud Functions
*   GCS
*   Bigquery

To keep complexity to a minimum, a makefile is used to provision the gcs buckets and bq datasets,
and maintenance should be completed manually using the gcp console. The cloud function is deployed
using a cloud build routine defined within the function folder.

## GCS Structure

Three gcs buckets are used by the project.

*   gs://{PROJECT_ID}-landing-zone/
*   gs://{PROJECT_ID}-archive/
*   gs://{PROJECT_ID}-processing-errors/

The cloud function is triggered by files being placed in the `landing-zone` bucket. Files handled
by the cloud function are then moved either to the archive bucket or the processing-errors bucket
depending on of they were successfully ingested into bigquery.

## BigQuery Structure

Datasets are provisioned on a per source basis. Raw datasets should be provisioned using the 
project makefile, while datasets for processed data (as views) can be provisioned either using the
same makefile, or directly using dbt.

