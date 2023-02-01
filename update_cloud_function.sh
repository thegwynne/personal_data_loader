#!/bin/bash

gcloud functions deploy personal_data_loader --region=europe-west2 --runtime=python38 --entry-point=main --trigger-bucket=cloudhal9000-landing-zone --source=functions/ingestor/
gsutil rsync schemas/ gs://cloudhal9000-schemas/