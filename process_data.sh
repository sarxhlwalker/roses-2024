#!/bin/bash

# Ensure you have all necessary libraries
# pip install -r requirements.txt

cd data_processing/
python3 assemble_outcomes.py $1 $3 $4 $5
Rscript mice.R $1 $2
