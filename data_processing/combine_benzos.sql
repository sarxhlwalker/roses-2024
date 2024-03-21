SELECT 
mid.stay_id, mid.time_interval, (mid.drug_dose / 2) + lor.drug_dose + (dia.drug_dose / 5) as drug_dose, mid.female, mid.age, mid.height,
mid.weight, mid.admit_year, mid.dementia, mid.tbi, mid.sud, mid.white, mid.black, mid.asian, mid.hispanic, mid.cardiac, mid.med_surg, mid.medicare, mid.medicaid, mid.english, mid.heart_rate, mid.resp_rate, mid.fio2, mid.spo2, mid.sbp, mid.dbp, mid.haloperidol, mid.ketamine, mid.dexmedetomidine, mid.vasopressor, mid.opioid, mid.gcs, mid.lorazepam, mid.propofol, mid.neuro, mid.min_riker, mid.max_riker, mid.pao2, mid.paco2, mid.propofol_24h, mid.benzo_24h
FROM `roses-0.roses.Midazolam_assembled` as mid
FULL JOIN `roses-0.roses.Lorazepam_assembled` as lor on lor.stay_id = mid.stay_id and lor.time_interval = mid.time_interval
FULL JOIN `roses-0.roses.Diazepam_assembled` as dia on dia.stay_id = mid.stay_id and dia.time_interval = mid.time_interval
order by 1, 2
