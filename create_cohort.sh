#!/bin/bash

# Generates tables used for ROSES study on a specified dataset on BigQuery
# Make sure you have created a "roses" dataset on your GCP project
BQ_OPTIONS_1="--use_legacy_sql=False --replace --max_rows=5"
BQ_OPTIONS_2="--project_id $1 --dataset_id roses"

BQ_OPTIONS="--use_legacy_sql=False --replace --max_rows=5 --project_id $1 --dataset_id roses" 

bq query $BQ_OPTIONS_1 $BQ_OPTIONS_2 --destination_table=baseline_vent_duration_mimic_24h < create_cohorts/baseline_vent_duration_mimic.sql
bq query $BQ_OPTIONS_1 $BQ_OPTIONS_2 --destination_table=mimic_eligibility_24h < create_cohorts/create_main_cohort/mimic_eligibility_24h.sql
bq query $BQ_OPTIONS_1 $BQ_OPTIONS_2 --destination_table=baseline_mimic_24h < create_cohorts/create_main_cohort/baseline.sql
bq query $BQ_OPTIONS_1 --parameter=time_int:INT64:240 $BQ_OPTIONS_2 --destination_table=mimic_inputevents_drug_group_from6 < create_cohorts/create_main_cohort/mimic_inputevents_drugs.sql
bq query $BQ_OPTIONS_1 --parameter=time_int:INT64:240 --parameter=one_week:INT64:10080 $BQ_OPTIONS_2 --destination_table=mimic_time_varying_from6 < create_cohorts/create_main_cohort/mimic_time_varying.sql
bq query $BQ_OPTIONS --destination_table=baseline_time_varying_from6 < create_cohorts/create_main_cohort/baseline_time_varying.sql

# You will have to manually download these files.
