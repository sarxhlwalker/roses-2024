#!/bin/bash

# Ensure you have all necessary libraries
# pip install -r requirements.txt

cd scripts/
python3 assemble_outcomes.py $1 $3 $4 $5
Rscript mice_all_patients.R $1 $2
