WITH first_pass AS (
  SELECT DISTINCT total_eligibility.stay_id, total_eligibility.hadm_id, total_eligibility.subject_id, total_eligibility.time_zero, admissions.race AS race_original, MIMIC_patient.anchor_age AS age, total_eligibility.female_clean AS female, total_eligibility.race_clean AS ethnicity, MIMIC_height.height AS height, weight_admit AS weight, -- weight upon admittance
CAST(SUBSTRING(MIMIC_patient.anchor_year_group, 1, 4) AS int64) AS admit_year, -- keeping first year of year group
CASE 
  WHEN regexp_contains(icd_code,"^F[0-3]+") OR regexp_contains(icd_code, "^290") THEN 1
  ELSE 0 
END AS dementia,
CASE  
  WHEN (
    regexp_contains(icd_code, "^S020.*") or   -- icd 10
    regexp_contains(icd_code, "^S021.*") or
    regexp_contains(icd_code, "^S028[1-3].*") or
    regexp_contains(icd_code, "^S0291.*") or
    regexp_contains(icd_code, "^S040[2-4].*") or
    regexp_contains(icd_code, "^S071.*") or
    regexp_contains(icd_code, "^T744.*") or
    regexp_contains(icd_code, "^80[1-2].*") or    -- icd 9
    regexp_contains(icd_code, "^80[3-4].*") or
    regexp_contains(icd_code, "^85[0-4].*") or
    regexp_contains(icd_code, "^854[0-1].*") or
    regexp_contains(icd_code, "^950[1-3].*") or
    regexp_contains(icd_code, "^95901$") or
    regexp_contains(icd_code, "^99555$")
  ) then 1
  else 0
END AS tbi, 
CASE 
  WHEN (
    regexp_contains(icd_code, "^291") or    -- alcohol substance use disorder
    regexp_contains(icd_code, "^3050") or
    regexp_contains(icd_code, "^3575") or
    regexp_contains(icd_code, "^303(0|9)") or
    regexp_contains(icd_code, "^4255") or
    regexp_contains(icd_code, "^5353") or
    regexp_contains(icd_code, "^571[0-3]") or
    regexp_contains(icd_code, "^6554") or
    regexp_contains(icd_code, "^76071") or
    regexp_contains(icd_code, "^980[0-1]") or
    regexp_contains(icd_code, "^E860([0-2]|9)") or
    regexp_contains(icd_code, "^G621") or
    regexp_contains(icd_code, "^G312") or
    regexp_contains(icd_code, "^I426") or
    regexp_contains(icd_code, "^X(4|6)5") or
    regexp_contains(icd_code, "^K292") or
    regexp_contains(icd_code, "^K70([0-4]|9)") or
    regexp_contains(icd_code, "^K852") or
    regexp_contains(icd_code, "^(K|Q)860") or
    regexp_contains(icd_code, "^P043") or
    regexp_contains(icd_code, "^Y15") or 
    regexp_contains(icd_code, "^6483[0-4]") or   -- other
    regexp_contains(icd_code, "^30(4[0-9]|5[2-7]|59)[0-3]") or 
    regexp_contains(icd_code, "^292") or
    regexp_contains(icd_code, "^9650") or
    regexp_contains(icd_code, "^E85(00|41)") or
    regexp_contains(icd_code, "^E93(50|96)") or
    regexp_contains(icd_code, "^(E03|96)85") or
    regexp_contains(icd_code, "^V6542") or
    regexp_contains(icd_code, "^F1[0-9]")
  ) then 1
  ELSE 0
END AS sud, 
CASE 
  WHEN icustays.first_careunit LIKE "Neuro%" OR icustays.first_careunit LIKE "Trauma%" THEN "NEURO-TRAUMA"
  WHEN icustays.first_careunit LIKE "Coronary%" OR icustays.first_careunit LIKE "Cardiac%" THEN "CARDIAC"
  WHEN icustays.first_careunit LIKE "Surgical%" OR icustays.first_careunit LIKE "Medical%" THEN "MEDICAL-SURGICAL"
  ELSE NULL 
END AS icutype,
CASE 
  WHEN MIMIC_patient.dod IS NOT NULL 
    THEN (DATETIME_DIFF(MIMIC_patient.dod, icustays.intime, MINUTE) - time_zero) / (60 * 24)
  ELSE NULL
END AS death_offset_days, mimic_vent_duration.duration AS vent_duration,
DATETIME_DIFF(admissions.dischtime, admissions.admittime, MINUTE) / (60 * 24) AS hosp_los_days,
icustays.los AS icu_los_days,
CASE 
  WHEN language like "ENGLISH" THEN 1
  ELSE 0
END AS english, 
insurance,
data_source
FROM `roses-0.roses.mimic_eligibility_24h` AS total_eligibility -- 24h dataset
LEFT JOIN `physionet-data.mimiciv_hosp.patients` AS MIMIC_patient ON total_eligibility.subject_id = CAST(MIMIC_patient.subject_id AS string)
LEFT JOIN `physionet-data.mimiciv_derived.first_day_weight` AS MIMIC_weight ON total_eligibility.stay_id = MIMIC_weight.stay_id
LEFT JOIN `physionet-data.mimiciv_hosp.diagnoses_icd` AS diagnoses_icd ON total_eligibility.subject_id = CAST(diagnoses_icd.subject_id AS string)
LEFT JOIN `physionet-data.mimiciv_icu.icustays` AS icustays ON total_eligibility.stay_id = icustays.stay_id
LEFT JOIN `physionet-data.mimiciv_hosp.admissions` AS admissions ON admissions.hadm_id = total_eligibility.hadm_id
LEFT JOIN `physionet-data.mimiciv_derived.height` AS MIMIC_height ON MIMIC_height.stay_id = total_eligibility.stay_id
LEFT JOIN `roses-0.roses.baseline_vent_duration_mimic_24h` AS mimic_vent_duration ON mimic_vent_duration.stay_id = total_eligibility.stay_id
WHERE eligible = 1
)

SELECT DISTINCT stay_id, hadm_id, subject_id, time_zero, female, race_original, ethnicity, MAX(english) OVER (PARTITION BY stay_id) as english, insurance, age, height, weight, admit_year, MAX(dementia) OVER (PARTITION BY stay_id) AS dementia, MAX(tbi) OVER (PARTITION BY stay_id) as tbi, MAX(sud) OVER (PARTITION BY stay_id) as sud, icutype, death_offset_days, vent_duration, hosp_los_days, icu_los_days, data_source
FROM first_pass 
WHERE death_offset_days >= 0 or death_offset_days is null
