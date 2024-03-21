WITH pdx AS
(SELECT el.*, icustays.intime, 
case
  when el.death_offset_days is not null and (el.death_offset_days * 24 * 60) < (el.time_zero + baseline_24h.vent_duration) then (el.death_offset_days * 24 * 60) - el.time_zero 
  else baseline_24h.vent_duration
end as last_time -- minutes after time_zero 
FROM `roses-0.roses.mimic_eligibility_24h` AS el -- for 24h dataset
LEFT JOIN `physionet-data.mimiciv_icu.icustays` AS icustays ON el.stay_id = icustays.stay_id
LEFT JOIN `roses-0.roses.baseline_mimic_24h` as baseline_24h ON baseline_24h.stay_id = el.stay_id
WHERE eligible = 1),

-- get maxed values per time
first as 
(SELECT stay_id, time_interval, variable, MAX(value) as value 
FROM (
  SELECT pdx.stay_id, FLOOR((DATETIME_DIFF(ce.charttime, intime, MINUTE) - pdx.time_zero) / @time_int) AS time_interval, last_time,
  CASE WHEN ce.itemid IN (220210,224690) AND valuenum > 0 AND valuenum < 70 THEN "resp_rate" 
  WHEN ce.itemid IN (220045) AND valuenum > 0 AND valuenum < 300 THEN "heart_rate"
  WHEN ce.itemid IN (223835) AND valuenum >= 21 and valuenum <= 100 THEN "inspired oxygen fraction"
  when ce.itemid in (220235) AND valuenum >= 0 then "PaCO2"
  END AS variable, 
  CASE
    WHEN ce.itemid in (220210,224690) and valuenum > 0 and valuenum < 70 then valuenum
    WHEN ce.itemid in (220045) and valuenum > 0 and valuenum < 300 then valuenum
    WHEN ce.itemid in (223835) and valuenum >= 21 and valuenum <= 100 THEN valuenum 
    WHEN ce.itemid in (220235) AND valuenum >= 0 then valuenum 
  END AS value
  FROM pdx
  LEFT JOIN (
    SELECT * 
    FROM `physionet-data.mimiciv_icu.chartevents` AS ce
    WHERE ce.itemid IN (
      220210,224690 -- Respiratory rate
    , 220045 -- heart rate
    , 223835 -- inspired oxygen fraction
    , 220235 -- paco2 
    )
  ) AS ce ON pdx.stay_id = ce.stay_id
  WHERE data_source LIKE "MIMIC_IV" and DATETIME_DIFF(ce.charttime, intime, MINUTE) >= (time_zero - 6*@time_int)
) 
where variable is not null and value is not null 
  AND time_interval <= @one_week/@time_int -- only events in first 7 days
  AND time_interval <= last_time/@time_int -- only events before first extubation 
GROUP BY 1, 2, 3

UNION ALL

-- get min'd values per time
SELECT stay_id, time_interval, variable, MIN(value) AS value
FROM ( 
  SELECT pdx.stay_id, FLOOR((DATETIME_DIFF(ce.charttime, intime, MINUTE) - pdx.time_zero) / @time_int) AS time_interval, last_time,
  CASE
    WHEN ce.itemid IN (220277) AND 0 <= valuenum AND valuenum <= 100 THEN "peripheral oxygen saturation"
    WHEN ce.itemid IN (227243, 225309, 224167, 220179, 220050) THEN "systolic blood pressure"
    WHEN ce.itemid IN (220051, 220180, 224643, 225310, 227242) THEN "diastolic blood pressure"
    WHEN ce.itemid IN (223900) THEN "GCS - verbal"
    WHEN ce.itemid IN (223901) THEN "GCS - motor"
    WHEN ce.itemid IN (220739) THEN "GCS - eye"
    WHEN ce.itemid IN (220224) THEN "PaO2"
  END AS variable, 
  CASE 
    WHEN ce.itemid IN (220277) AND 0 <= valuenum AND valuenum <= 100 THEN valuenum
    WHEN ce.itemid IN (227243, 225309, 224167, 220179, 220050) AND valuenum <= 300 THEN valuenum
    WHEN ce.itemid IN (220051, 220180, 224643, 225310, 227242) AND valuenum <= 200 THEN valuenum
    WHEN ce.itemid IN (223900) THEN valuenum
    WHEN ce.itemid IN (223901) THEN valuenum
    WHEN ce.itemid IN (220739) THEN valuenum
    WHEN ce.itemid IN (220224) AND valuenum >= 0 THEN valuenum
  END AS value
  FROM pdx
  LEFT JOIN (
    SELECT * 
    FROM `physionet-data.mimiciv_icu.chartevents` AS ce
    WHERE ce.itemid IN (
      220277 -- peripheral o2 saturation 
      , 227243, 225309, 224167, 220179, 220050 -- systolic blood pressure
      , 220051, 220180, 224643, 225310, 227242 -- diastolic blood pressure
      , 223900, 223901, 220739 -- gcs
      , 220224 -- PaO2
    )
  ) AS ce ON pdx.stay_id = ce.stay_id 
  where DATETIME_DIFF(ce.charttime, intime, MINUTE) >= (time_zero - 6*@time_int) 
)
where variable is not null and value is not null 
  AND time_interval <= @one_week/@time_int -- only events in first 7 days
  AND time_interval <= last_time/@time_int -- only events before first extubation 
GROUP BY 1, 2, 3

UNION ALL

-- is there invasive ventilation 
SELECT stay_id, time_interval, "Invasive Ventilation" AS variable, MAX(value) AS value
FROM (
  SELECT pdx.stay_id, 
  FLOOR((DATETIME_DIFF(vent.starttime, intime, MINUTE) - pdx.time_zero) / @time_int) AS time_interval, last_time,
  1 AS value
  FROM pdx
  LEFT JOIN `physionet-data.mimiciv_derived.ventilation` AS vent ON vent.stay_id = pdx.stay_id
  WHERE ventilation_status LIKE "InvasiveVent" -- exclude tracheostomy? 
  and DATETIME_DIFF(vent.starttime, intime, MINUTE) >= (time_zero - 6*@time_int)

  UNION ALL 

  SELECT pdx.stay_id, -- the interval AFTER endtime won't have invasive vent. unless another row 
  FLOOR((DATETIME_DIFF(vent.endtime, intime, MINUTE) - pdx.time_zero) / @time_int) + 1 AS time_interval, last_time,
  0 AS value
  FROM pdx
  LEFT JOIN `physionet-data.mimiciv_derived.ventilation` AS vent ON vent.stay_id = pdx.stay_id
  WHERE ventilation_status LIKE "InvasiveVent" -- exclude tracheostomy? 
  and DATETIME_DIFF(vent.endtime, intime, MINUTE) >= (time_zero - 6*@time_int)
)
WHERE time_interval <= @one_week/@time_int -- only events in first 7 days
AND time_interval <= last_time/@time_int -- only events before first extubation 
GROUP BY 1, 2, 3

UNION ALL 

-- is there extubation 
SELECT stay_id, time_interval, "Extubation" AS variable, MAX(value) AS value
FROM (
  SELECT pdx.stay_id, 
  FLOOR((DATETIME_DIFF(vent.starttime, intime, MINUTE) - pdx.time_zero) / @time_int) AS time_interval, last_time,
  1 AS value
  FROM pdx
  LEFT JOIN `physionet-data.mimiciv_derived.ventilation` AS vent ON vent.stay_id = pdx.stay_id
  WHERE ventilation_status IN ("SupplementalOxygen", "HFNC", "NonInvasiveVent", "None")
    and DATETIME_DIFF(vent.starttime, intime, MINUTE) >= (time_zero - 6*@time_int)

  UNION ALL 

  SELECT pdx.stay_id, 
  FLOOR((DATETIME_DIFF(vent.endtime, intime, MINUTE) - pdx.time_zero) / @time_int) AS time_interval, last_time,
  0 AS value
  FROM pdx
  LEFT JOIN `physionet-data.mimiciv_derived.ventilation` AS vent ON vent.stay_id = pdx.stay_id
  WHERE ventilation_status IN ("SupplementalOxygen", "HFNC", "NonInvasiveVent", "None")
    and DATETIME_DIFF(vent.endtime, intime, MINUTE) >= (time_zero - 6*@time_int)
)
WHERE time_interval <= @one_week/@time_int -- only events in first 7 days
AND time_interval <= last_time/@time_int -- only events before first extubation 
GROUP BY 1, 2, 3

UNION ALL 

-- get total_value of drug per time value from inputevents

SELECT *
-- FROM `roses-0.roses.mimic_inputevents_drug_group` -- regular dataset
FROM `roses-0.roses.mimic_inputevents_drug_group_from6` -- 24h dataset
where time_interval <= @one_week/@time_int -- only get values within first 7 days

union all

-- is there a max sedation scale
select distinct stay_id, time_interval, "Max Riker" as variable, max(value) as value
from (
  select pdx.stay_id, FLOOR((DATETIME_DIFF(charttime, intime, MINUTE) - time_zero) / @time_int) as time_interval, 
  case
    when itemid = 223753 then valuenum
    when itemid = 228096 and valuenum = -5 then 1
    when itemid = 228096 and valuenum = -4 then 2
    when itemid = 228096 and valuenum in (-3, -2) then 3
    when itemid = 228096 and valuenum in (-1, 0, 1) then 4
    when itemid = 228096 and valuenum = 2 then 5
    when itemid = 228096 and valuenum = 3 then 6
    when itemid = 228096 and valuenum = 4 then 7
  end as value, last_time
  from pdx 
  left join `physionet-data.mimiciv_icu.chartevents` as ce on ce.stay_id = pdx.stay_id
  where ce.itemid in (
    228096, -- richmond
    -- 228299, -- richmond goal
    223753 -- riker
    -- 228710 -- goal
  ) and eligible = 1 and DATETIME_DIFF(charttime, intime, MINUTE) >= (time_zero - 6*@time_int)
)
WHERE time_interval <= @one_week/@time_int -- only events in first 7 days 
and time_interval <= last_time/@time_int -- only events before first extubation 
group by 1, 2, 3

union all 

-- is there a min sedation scale 
select distinct stay_id, time_interval, "Min Riker" as variable, min(value) as value
from (
  select pdx.stay_id, FLOOR((DATETIME_DIFF(charttime, intime, MINUTE) - time_zero) / @time_int) as time_interval, 
  case
    when itemid = 223753 then valuenum
    when itemid = 228096 and valuenum = -5 then 1
    when itemid = 228096 and valuenum = -4 then 2
    when itemid = 228096 and valuenum in (-3, -2) then 3
    when itemid = 228096 and valuenum in (-1, 0, 1) then 4
    when itemid = 228096 and valuenum = 2 then 5
    when itemid = 228096 and valuenum = 3 then 6
    when itemid = 228096 and valuenum = 4 then 7
  end as value, last_time
  from pdx 
  left join `physionet-data.mimiciv_icu.chartevents` as ce on ce.stay_id = pdx.stay_id
  where ce.itemid in (
    228096, -- richmond
    223753 -- riker
  ) and eligible = 1 and DATETIME_DIFF(charttime, intime, MINUTE) >= (time_zero - 6*@time_int)
)
WHERE time_interval <= @one_week/@time_int -- only events in first 7 days 
and time_interval <= last_time/@time_int -- only events before first extubation 
group by 1, 2, 3

union all 

-- is there a min sedation scale goal
select distinct stay_id, time_interval, "Min Riker Goal" as variable, min(value) as value
from (
  select pdx.stay_id, FLOOR((DATETIME_DIFF(charttime, intime, MINUTE) - time_zero) / @time_int) as time_interval, 
  case
    when itemid = 228710 then valuenum
    when itemid = 228299 and valuenum = -5 then 1
    when itemid = 228299 and valuenum = -4 then 2
    when itemid = 228299 and valuenum in (-3, -2) then 3
    when itemid = 228299 and valuenum in (-1, 0, 1) then 4
    when itemid = 228299 and valuenum = 2 then 5
    when itemid = 228299 and valuenum = 3 then 6
    when itemid = 228299 and valuenum = 4 then 7
  end as value, last_time
  from pdx 
  left join `physionet-data.mimiciv_icu.chartevents` as ce on ce.stay_id = pdx.stay_id
  where ce.itemid in (
    228299, -- richmond goal
    228710 -- goal
  ) and eligible = 1 and DATETIME_DIFF(charttime, intime, MINUTE) >= (time_zero - 6*@time_int)
)
WHERE time_interval <= @one_week/@time_int -- only events in first 7 days 
and time_interval <= last_time/@time_int -- only events before first extubation 
group by 1, 2, 3

union all 

-- are there neuromuscular blockers 
SELECT stay_id, time_interval, variable, MAX(value) AS value
FROM (
  select stay_id, time_interval, "Neuromuscular blocker" AS variable, value from (SELECT pdx.stay_id, 
  FLOOR((DATETIME_DIFF(ce.charttime, intime, MINUTE) - pdx.time_zero) / @time_int) AS time_interval, last_time,
  1 AS value
  FROM pdx
  LEFT JOIN `physionet-data.mimiciv_icu.chartevents` AS ce ON ce.stay_id = pdx.stay_id
  WHERE ce.itemid in (
    229788
  )
  ) where time_interval <= last_time/@time_int and time_interval <= @one_week/@time_int

  UNION ALL 

  SELECT stay_id, time_interval, variable as string, value
  from `roses-0.roses.mimic_inputevents_drug_group_from6` AS inp 
  WHERE variable like "Neuromuscular blocker"
)
GROUP BY 1, 2, 3)

, second as 
(select *, 
first_value(time_interval) over (partition by stay_id order by time_interval asc) as first_extub
from first
where variable like "Extubation" and value = 1
)

, third as (
select first.stay_id, first.time_interval, first.variable, first.value, 
case 
  when first_extub is not null then first_extub 
  else null
end as first_extub
from first 
left join second on second.stay_id = first.stay_id
left join `roses-0.roses.mimic_eligibility_24h` AS el on el.stay_id = first.stay_id 
where eligible = 1 and (first.time_interval < first_extub or first_extub is null))

select distinct stay_id, time_interval, variable, value
from third
ORDER BY 1, 2, 3 ASC
