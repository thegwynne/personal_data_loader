#!/bin/bash

curl  -H "Authorization: bearer $(gcloud auth print-identity-token)" https://europe-west2-cloudhal9000.cloudfunctions.net/octopus_fetcher
