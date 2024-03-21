-- mimic ventilation after 24h
-- copy of mimic_eligibility with minor changes

WITH first_pass AS (SELECT DISTINCT icustays.stay_id, icustays.hadm_id, icustays.subject_id,
CASE 
  WHEN gender LIKE "F" THEN 1
  WHEN gender LIKE "M" THEN 0
  ELSE NULL
END AS female_clean,
CASE 
  WHEN gender LIKE "F" THEN 1
  WHEN gender LIKE "M" THEN 1
  ELSE 0
END AS gender_inclusion
FROM `physionet-data.mimiciv_icu.icustays` AS icustays
LEFT JOIN `physionet-data.mimiciv_hosp.patients` AS patients ON icustays.subject_id = patients.subject_id
-- LEFT to keep all icustays, even if no patient data
ORDER BY icustays.stay_id),

second_pass AS (
  SELECT DISTINCT first_pass.stay_id, first_pass.hadm_id, first_pass.subject_id, female_clean, gender_inclusion, 
  CASE 
    -- some stay_ids have multiple entires where race is not consistent
    WHEN race LIKE "WHITE" THEN 1
    WHEN race LIKE "PORTUGESE" THEN 1
    WHEN race LIKE "BLACK%" THEN 1
    WHEN race LIKE "HISPANIC/LATINO%" THEN 1
    WHEN race LIKE "HISPANIC OR LATINO" THEN 1
    WHEN race LIKE "ASIAN%" THEN 1
    WHEN race LIKE "AMERICAN INDIAN%" THEN 1
    WHEN race LIKE "%ISLANDER%" THEN 1
    WHEN race LIKE "SOUTH AMERICAN" THEN 1
    ELSE 0
  END AS race_inclusion, 
  CASE 
    WHEN race LIKE "WHITE" THEN "WHITE"
    WHEN race LIKE "PORTUGESE" THEN "WHITE"
    WHEN race LIKE "BLACK%" THEN "BLACK"
    WHEN race LIKE "HISPANIC/LATINO%" THEN "HISPANIC"
    WHEN race LIKE "HISPANIC OR LATINO" THEN "HISPANIC"
    WHEN race LIKE "ASIAN%" THEN "ASIAN"
    WHEN race LIKE "AMERICAN INDIAN%" THEN "INDIGENOUS"
    WHEN race LIKE "%ISLANDER%" THEN "INDIGENOUS"
    WHEN race LIKE "SOUTH AMERICAN" THEN "HISPANIC"
  ELSE NULL
  END AS race_clean
  FROM first_pass
  LEFT JOIN `physionet-data.mimiciv_hosp.admissions` AS adm ON adm.subject_id = first_pass.subject_id
),

-- add ventilation data 
third_pass AS (
  SELECT DISTINCT second_pass.stay_id, second_pass.hadm_id, CAST(second_pass.subject_id AS string) AS subject_id, icustays.intime, female_clean, gender_inclusion, MAX(race_inclusion) OVER (PARTITION BY second_pass.stay_id) AS race_inclusion, FIRST_VALUE(race_clean) OVER (PARTITION BY second_pass.stay_id ORDER BY race_clean DESC) AS race_clean, 
  vent_duration.starttime as time_zero,
  vent_duration.duration as duration, 
  FROM second_pass 
  LEFT JOIN `roses-0.roses.baseline_vent_duration_mimic_24h` as vent_duration on vent_duration.stay_id = second_pass.stay_id
  LEFT JOIN `physionet-data.mimiciv_icu.icustays` AS icustays ON second_pass.stay_id = icustays.stay_id
), 

sixth_pass as (
  select third_pass.*, vent.ventilation_status, 
  DATETIME_DIFF(vent.starttime, intime, MINUTE) as trach_start, 
  DATETIME_DIFF((SELECT MIN(starttime) -- get first starttime that satisfies the below requirements
  FROM `physionet-data.mimiciv_derived.ventilation` AS v2
  WHERE vent.stay_id = v2.stay_id AND v2.starttime > vent.starttime AND (
    v2.ventilation_status NOT LIKE "Tracheostomy" -- (a) not trach
    OR DATETIME_DIFF(v2.starttime, vent.endtime, DAY) > 2  -- (b) trach, but new starttime > 48h prev. end
    )
  ), intime, MINUTE) as trach_end, 
  DATETIME_DIFF(vent.endtime, intime, MINUTE) as official_end
  from third_pass
  left join `physionet-data.mimiciv_derived.ventilation` as vent on vent.stay_id = third_pass.stay_id
), 

seventh_pass as (
  select stay_id, hadm_id, subject_id, female_clean, gender_inclusion, race_inclusion, race_clean, time_zero, ventilation_status, trach_start, trach_end, official_end,
  case 
    when trach_end is not null 
      and ventilation_status like "Tracheostomy" and trach_start >= -720 
      -- not earlier than 12h of intime 
      AND trach_start < time_zero - 24*60
      AND trach_end <= time_zero
      -- if trach overlaps with any part of the first 24h 
      THEN 1
    when trach_end is null 
      and ventilation_status like "Tracheostomy" and trach_start >= -720 
      -- not earlier than 12h of intime 
      AND trach_start < time_zero - 24*60
      AND official_end <= time_zero
      -- if trach overlaps with any part of the first 24h 
      THEN 1
    else 0
  end as trach
  from sixth_pass
), 

eight_pass as (
  SELECT DISTINCT stay_id, hadm_id, subject_id, female_clean, gender_inclusion, race_inclusion, race_clean, time_zero,
  MAX(trach) OVER (PARTITION BY stay_id) AS trach
  from seventh_pass 
)

, nine as (
  SELECT DISTINCT eight_pass.*, 
  CASE 
    WHEN MIMIC_patient.dod IS NOT NULL and time_zero is not null 
      THEN (DATETIME_DIFF(MIMIC_patient.dod, icustays.intime, MINUTE) - time_zero) / (60 * 24)
    ELSE NULL
  END AS death_offset_days,
  'MIMIC_IV' AS data_source, 
  case 
    when vent.ventilation_status like "InvasiveVent" then 1 else 0
  end as ventilation_inclusion
  FROM eight_pass
  left join `physionet-data.mimiciv_icu.icustays` as icustays on icustays.stay_id = eight_pass.stay_id 
  left join `physionet-data.mimiciv_hosp.patients` as MIMIC_patient on CAST(MIMIC_patient.subject_id as string) = eight_pass.subject_id
  left join `physionet-data.mimiciv_derived.ventilation` as vent on vent.stay_id = eight_pass.stay_id and vent.ventilation_status like "InvasiveVent"
)

, ten as (
  select stay_id, hadm_id, subject_id, female_clean, gender_inclusion, race_inclusion, race_clean, time_zero, trach, death_offset_days, max(ventilation_inclusion) as ventilation_inclusion, data_source
  from nine
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12
)

-- restrict patients who have multiple stays
, eleven as (
  select distinct ten.stay_id, hadm_id, ten.subject_id, female_clean, gender_inclusion, race_inclusion, race_clean, time_zero, trach, death_offset_days, ventilation_inclusion, data_source, 
    case
      when additional.stay_id = ten.stay_id then 1
      else 0
    end as is_first
    from ten 
    left join (
      select first_value(stay_id) over (partition by subject_id order by stay_id) as stay_id, subject_id
      from ten 
    ) as additional on additional.subject_id = ten.subject_id 
)

, next as (select *, 
case 
	when ventilation_inclusion = 1 and gender_inclusion = 1 and race_inclusion = 1 and (death_offset_days >= 0 or death_offset_days is null) and trach = 0 and time_zero is not null and is_first = 1 then 1 
	else 0 
end as eligible
from eleven
ORDER BY eligible DESC)

select * 
from next 
