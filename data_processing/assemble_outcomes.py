import pandas as pd
from google.cloud import bigquery
import os
import sys

import numpy as np

client = bigquery.Client()

DIR = ""  # Can change this depending on directory structure
OUTPUT_DIR = ""
DRUG_OUTCOMES = {
    "Propofol",
    "Midazolam",
    "Lorazepam",    # R script will combine benzos
    "Dexmedetomidine",
    "Diazepam",
    "Min Riker"
}

# OUTCOME_FILE = 'outcomes.csv'
# BASELINE_FILE = 'baseline_mimic_24h.csv'
# TIME_VARYING_FILE = 'mimic_time_varying_from6.csv'
# BASELINE_TIME_VARYING = 'baseline_time_varying_from6.csv'

OUTCOME_FILE = sys.argv[1]
BASELINE_FILE = sys.argv[2]
TIME_VARYING_FILE = sys.argv[3]
BASELINE_TIME_VARYING = sys.argv[4]


def apply_baseline(df):
    df['white'] = df.apply(lambda row: 1 if row.ethnicity == 'WHITE' else 0, axis=1)
    df['black'] = df.apply(lambda row: 1 if row['ethnicity'] == 'BLACK' else 0, axis=1)
    df['asian'] = df.apply(lambda row: 1 if row['ethnicity'] == 'ASIAN' else 0, axis=1)
    df['hispanic'] = df.apply(lambda row: 1 if row['ethnicity'] == 'HISPANIC' else 0, axis=1)
    # indigenous as last group

    df['cardiac'] = df.apply(lambda row: 1 if row['icutype'] == 'CARDIAC' else 0, axis=1)
    df['med_surg'] = df.apply(lambda row: 1 if row['icutype'] == 'MEDICAL-SURGICAL' else 0, axis=1)
    # neuro trauma as last group

    df['medicare'] = df.apply(lambda row: 1 if row['insurance'] == 'Medicare' else 0, axis=1)
    df['medicaid'] = df.apply(lambda row: 1 if row['insurance'] == 'Medicaid' else 0, axis=1)
    # Other as last group

    return df


def read_csv(drug: str, baseline):
    tv = pd.read_csv(f"{DIR}/{TIME_VARYING_FILE}")
    df = tv[tv['time_interval'] >= 0][["stay_id", "time_interval"]].drop_duplicates()
    df = df.merge(tv[(tv["time_interval"] >= 0) & (tv['variable'] == drug)][['stay_id', 'time_interval', 'value']],
                 how='left', on=['stay_id', 'time_interval'])
    df = df.merge(baseline, how="right", on="stay_id")

    df = apply_baseline(df)

    # if no row for time interval 0 exists, do it manually (a bit hacky)
    for i in (35349828, 37136992):  # 32442329
        row = pd.DataFrame([{
            'stay_id': i,
            'time_interval': 0
        }])
        row = row.merge(baseline, how='left', on='stay_id')
        if i == 37136992:
            row['med_surg'] = 1
        else:
            row['med_surg'] = 0
        row['cardiac'] = 0
        row['white'] = 1
        row['black'] = 0
        row['hispanic'] = 0
        row['asian'] = 0
        row['medicare'] = 1
        row['medicaid'] = 0
        df = pd.concat([df, row])
        df = df.reset_index(drop=True)

    btv = pd.read_csv(f"{DIR}/{BASELINE_TIME_VARYING}")[['stay_id', 'propofol_24h', 'benzo_24h']]
    df = df.merge(btv, how='left', on='stay_id')

    df = df.drop(['data_source', 'hadm_id', 'subject_id', 'race_original', 'insurance',
                  'ethnicity', 'icutype'], axis=1)

    df.loc[df['time_interval'].isna(), ['time_interval']] = 0
    if drug != "Min Riker":
        df.loc[df['value'].isna(), ['value']] = 0
    return df


def load_baseline_mimic():
    base_mimic = pd.read_csv(f"{DIR}/{BASELINE_FILE}")
    return base_mimic


def bigquery_calls():
    for drug in DRUG_OUTCOMES:
        # upload table
        table_id = f"roses-0.roses.{drug}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )
        with open(f'../results/python/{drug}.csv', "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.
        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

        # gets other values
        with open('add_time_varying_to_baseline.sql', encoding="utf-8") as f:
            read_data = f.read()
        read_data = read_data.replace('`roses-0.Regression.propofol`', f"`roses-0.roses.{drug}`")
        table_id = f"roses-0.roses.{drug}_assembled"
        job_config = bigquery.QueryJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            destination=table_id
        )
        query_job = client.query(
            read_data, job_config=job_config
        )  # Make an API request.
        query_job.result()  # Wait for the job to complete.
        if drug not in ("Midazolam", "Lorazepam", "Diazepam"):
            table = client.get_table(table_id)  # Make an API request.
            df = client.list_rows(table).to_dataframe()
            df.to_csv(f'../results/python/{drug}_assembled.csv', index=False)
            print(f"{drug} complete")

    # combine benzos into units of lorazepam
    with open('combine_benzos.sql', encoding="utf-8") as f:
        read_data = f.read()
    read_data = read_data.replace('`roses-0.Regression.`', "`roses-0.roses.`")
    table_id = f"roses-0.roses.benzo_assembled"
    job_config = bigquery.QueryJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        destination=table_id
    )
    query_job = client.query(
        read_data, job_config=job_config
    )  # Make an API request.
    query_job.result()  # Wait for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    df = client.list_rows(table).to_dataframe()
    df.to_csv(f'../results/python/benzo_assembled.csv', index=False)
    print("benzos complete")


if __name__ == "__main__":
    if not os.path.exists(DIR):
        os.mkdir(DIR)
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    base_mimic = load_baseline_mimic()
    for drug in DRUG_OUTCOMES:
        df = read_csv(drug, base_mimic)
        df.to_csv(f"{OUTPUT_DIR}/{drug}.csv", index=False)
        print(len(np.unique(df[(df['white'] == 1) | (df['black'] == 1) | (df['asian'] == 1) | (df['hispanic'] == 1)]['stay_id'])))

    bigquery_calls()

    df = pd.read_csv(f'{OUTPUT_DIR}/Propofol_assembled.csv')
    df['propofol_outcome'] = df['drug_dose']
    other = pd.read_csv(f'{OUTPUT_DIR}/benzo_assembled.csv')
    df = df.merge(other[['stay_id', 'time_interval', 'drug_dose']], how='left', on=['stay_id', 'time_interval'])
    df['benzo_outcome'] = df['drug_dose_y']
    df = df.drop(['drug_dose_y'], axis=1)
    other = pd.read_csv(f'{OUTPUT_DIR}/Ketamine_assembled.csv')
    df = df.merge(other[['stay_id', 'time_interval', 'drug_dose']], how='left', on=['stay_id', 'time_interval'])
    df['ketamine_outcome'] = df['drug_dose']
    df = df.drop(['drug_dose'], axis=1)
    other = pd.read_csv(f'{OUTPUT_DIR}/Dexmedetomidine_assembled.csv')
    df = df.merge(other[['stay_id', 'time_interval', 'drug_dose']], how='left', on=['stay_id', 'time_interval'])
    df['dexmedetomidine_outcome'] = df['drug_dose']
    df = df.drop(['drug_dose', 'drug_dose_x'], axis=1)
    other = pd.read_csv(f'{OUTPUT_DIR}/Min Riker_assembled.csv')
    df = df.merge(other[['stay_id', 'time_interval', 'drug_dose']], how='left', on=['stay_id', 'time_interval'])
    df['min_riker_outcome'] = df['drug_dose']
    df = df.drop(['drug_dose'], axis=1)

    df.to_csv(f"{OUTPUT_DIR}/{OUTCOME_FILE}", index=False)
