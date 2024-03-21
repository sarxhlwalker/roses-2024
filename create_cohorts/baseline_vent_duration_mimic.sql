WITH first_pass AS (
SELECT *,
  (SELECT MIN(starttime) -- get first starttime that satisfies the below requirements
  FROM `physionet-data.mimiciv_derived.ventilation` AS v2
  WHERE v1.stay_id = v2.stay_id AND v2.starttime > v1.starttime AND (
    v2.ventilation_status NOT LIKE "InvasiveVent" -- (a) not invasive
    OR DATETIME_DIFF(v2.starttime, v1.endtime, DAY) > 2  -- (b) invasive, but new starttime > 48h prev. end
    )
  ) AS next_time
FROM `physionet-data.mimiciv_derived.ventilation` AS v1
ORDER BY stay_id
),

-- convert starttime to offset; calculate duration
second_pass AS (
SELECT first_pass.stay_id, DATETIME_DIFF(starttime, icustays.intime, MINUTE) AS starttime,
CASE 
  WHEN next_time IS NOT NULL THEN DATETIME_DIFF(next_time, starttime, MINUTE)
  ELSE DATETIME_DIFF(endtime, starttime, MINUTE)
END AS duration
FROM first_pass
LEFT JOIN `physionet-data.mimiciv_icu.icustays` AS icustays ON icustays.stay_id = first_pass.stay_id
WHERE ventilation_status LIKE "InvasiveVent" and DATETIME_DIFF(starttime, icustays.intime, MINUTE) >= -720
-- invasive ventilation can't happen earlier than 12h before admit
)

-- get earliest starttime & curation per stay_id
SELECT DISTINCT stay_id, 
  FIRST_VALUE(starttime + 60*24) OVER (PARTITION BY stay_id ORDER BY starttime) AS starttime, 
  FIRST_VALUE(duration) OVER (PARTITION BY stay_id ORDER BY starttime) AS duration 
FROM second_pass
WHERE duration > (60*24)  -- must last for at least 24h 
