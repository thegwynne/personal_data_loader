#!/bin/bash

gcloud functions deploy octopus_fetcher --region=europe-west2 --runtime=python38 --entry-point=main --trigger-http --source=functions/octopus/ --set-secrets=api_details=octopus_api:latest
