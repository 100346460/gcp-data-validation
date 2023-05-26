#!/bin/bash
# sacred-truck-387712
# gcloud auth login
read -p 'Please input ProjectID: ' projectid

export GCP_PROJECT_ID="${projectid}"
export PROJECT_NUMBER=$(gcloud projects describe "${GCP_PROJECT_ID}" --format='get(projectNumber)')

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member user:daniel.cardinal.gcp2@gmail.com \
    --role roles/storage.admin

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com \
    --role roles/storage.admin

gcloud storage buckets create gs://test_data_validation

gsutil notification create -t projects/$GCP_PROJECT_ID/topics/test_data_validation_topic -f json -e OBJECT_FINALIZE gs://test_data_validation

gcloud beta pubsub subscriptions create process_test_data_validation --topic=test_data_validation_topic --project=sacred-truck-387712