#!/bin/bash
project_id="cloudhal9000"

gsutil mb -l europe-west2 -p ${project_id} gs://${project_id}-landing-zone/
gsutil mb -l europe-west2 -p ${project_id} gs://${project_id}-archive/
gsutil mb -l europe-west2 -p ${project_id} gs://${project_id}-processing-errors/

bq --location=EU mk --dataset --project_id=${project_id} pdl_spotify_raw