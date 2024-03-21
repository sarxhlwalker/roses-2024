with first_pass as (
select distinct drug.stay_id, drug.time_interval, drug.value as drug_dose, female, age, height, weight, admit_year, dementia, tbi, sud, death_offset_days, vent_duration, medicare, medicaid, english, hosp_los_days, icu_los_days, white, black, asian, hispanic,
cardiac, med_surg, heart_rate, resp_rate, fio2.value as fio2, fio2_time.value as fio2_time_val, fio2_time.time_interval as fio2_time_time, spo2.spo2, sbp.sbp, dbp.dbp, 
case when haloperidol.haloperidol is not null then haloperidol.haloperidol
  else 0 
end as haloperidol, 
case when fentanyl.fentanyl is not null then fentanyl.fentanyl
  else 0
end as fentanyl, 
case when hydromorphone.value is not null then hydromorphone.value
  else 0
end as hydromorphone, 
case when meperidine.value is not null then meperidine.value
  else 0 
end as meperidine,
case when morphine.value is not null then morphine.value
  else 0 
end as morphine, 
case when norepinephrine.value is not null then norepinephrine.value
  else 0 
end as norepinephrine, 
case when dopamine.value is not null then dopamine.value
  else 0
end as dopamine, 
case when vasopressin.value is not null then vasopressin.value
  else 0 
end as vasopressin, 
case when epinephrine.value is not null then epinephrine.value
  else 0 
end as epinephrine, 
case when dobutamine.value is not null then dobutamine.value
  else 0 
end as dobutamine, 
case when milrinone.value is not null then milrinone.value
  else 0 
end as milrinone, 
case 
  when norepinephrine.value is not null then norepinephrine.value
  when dopamine.value is not null then dopamine.value*0.01
  when vasopressin.value is not null then vasopressin.value*5
  when epinephrine.value is not null then epinephrine.value 
  else 0 
end as vasopressor, 
case 
  when hydromorphone.value is not null then hydromorphone.value*(10/1.5)
  when fentanyl.fentanyl is not null then fentanyl.fentanyl*100
  when morphine.value is not null then morphine.value
  when meperidine.value is not null then meperidine.value*(10/75)
  else 0 
end as opioid,
case 
  when eye.value is not null and motor.value is not null and verbal.value is not null then eye.value + motor.value + verbal.value
  else null
end as gcs, 
case 
  when eye_time.value is not null and motor_time.value is not null and verbal_time.value is not null then eye_time.value + motor_time.value + verbal_time.value
  else null
end as gcs_time_val,  
case 
  when eye_time.value is not null and motor_time.value is not null and verbal_time.value is not null then eye_time.time_interval 
  else null
end as gcs_time_time,  
pao2.value as pao2, pao2_time.value as pao2_time_val, pao2_time.time_interval as pao2_time_time, paco2.value as paco2, paco2_time.value as paco2_time_val, paco2_time.time_interval as paco2_time_time, case when neuro.value is not null then neuro.value else 0 end as neuro, 
min_riker.value as min_riker,
max_riker.value as max_riker, propofol_24h, benzo_24h,
case when propofol.value is not null and propofol.variable like "Propofol" then propofol.value else 0 end as propofol, 
case when propofol.value is not null and propofol.variable like "Ketamine" then propofol.value else 0 end as ketamine, 
case when propofol.value is not null and propofol.variable like "Dexmedetomidine" then propofol.value else 0 end as dexmedetomidine, 
case
  when benzo.variable like "Lorazepam" then benzo.value
  when benzo.variable like "Diazepam" then benzo.value * (1/5)
  when benzo.variable like "Midazolam" then benzo.value * (1/2)
  else 0 
end as lorazepam, 
min_riker_goal.value as min_riker_goal
from `roses-0.Regression.propofol` as drug   -- py script will replcae
left join (
  select stay_id, time_interval, value as heart_rate
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "heart_rate"
) as heart on heart.stay_id = drug.stay_id and heart.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value as resp_rate
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "resp_rate"
) as resp on resp.stay_id = drug.stay_id and resp.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "inspired oxygen fraction"
) as fio2 on fio2.stay_id = drug.stay_id and drug.time_interval - 1 = fio2.time_interval
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "inspired oxygen fraction"
) as fio2_time on fio2_time.stay_id = drug.stay_id and drug.time_interval = 0 and fio2_time.time_interval < -1
left join (
  select stay_id, time_interval, value as spo2
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "peripheral oxygen saturation"
) as spo2 on spo2.stay_id = drug.stay_id and spo2.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value as sbp   
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "systolic blood pressure"
) as sbp on sbp.stay_id = drug.stay_id and sbp.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value as dbp  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "diastolic blood pressure"
) as dbp on dbp.stay_id = drug.stay_id and dbp.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value as haloperidol  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Haloperidol"
) as haloperidol on haloperidol.stay_id = drug.stay_id and haloperidol.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value as fentanyl  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Fentanyl"
) as fentanyl on fentanyl.stay_id = drug.stay_id and fentanyl.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Hydromorphone"
) as hydromorphone on hydromorphone.stay_id = drug.stay_id and hydromorphone.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Morphine"
) as morphine on morphine.stay_id = drug.stay_id and morphine.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Meperidine"
) as meperidine on meperidine.stay_id = drug.stay_id and meperidine.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Norepinephrine"
) as norepinephrine on norepinephrine.stay_id = drug.stay_id and norepinephrine.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Dopamine"
) as dopamine on dopamine.stay_id = drug.stay_id and dopamine.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Vasopressin"
) as vasopressin on vasopressin.stay_id = drug.stay_id and vasopressin.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Epinephrine"
) as epinephrine on epinephrine.stay_id = drug.stay_id and epinephrine.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Dobutamine"
) as dobutamine on dobutamine.stay_id = drug.stay_id and dobutamine.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Milrinone"
) as milrinone on milrinone.stay_id = drug.stay_id and milrinone.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "GCS - eye"
) as eye on eye.stay_id = drug.stay_id and eye.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "GCS - verbal"
) as verbal on verbal.stay_id = drug.stay_id and verbal.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "GCS - motor"
) as motor on motor.stay_id = drug.stay_id and motor.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "GCS - eye"
) as eye_time on eye_time.stay_id = drug.stay_id and eye_time.time_interval < -1 and drug.time_interval = 0
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "GCS - verbal"
) as verbal_time on verbal_time.stay_id = drug.stay_id and verbal_time.time_interval < -1 and drug.time_interval = 0
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "GCS - motor"
) as motor_time on motor_time.stay_id = drug.stay_id and motor_time.time_interval < -1 and drug.time_interval = 0
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "PaO2"
) as pao2 on pao2.stay_id = drug.stay_id and pao2.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "PaO2"
) as pao2_time on pao2_time.stay_id = drug.stay_id and pao2_time.time_interval < -1 and drug.time_interval = 0
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "PaCO2"
) as paco2 on paco2.stay_id = drug.stay_id and paco2.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "PaCO2"
) as paco2_time on paco2_time.stay_id = drug.stay_id and paco2_time.time_interval < -1 and drug.time_interval = 0
left join (
  select stay_id, time_interval, value  
  from `roses-0.roses.mimic_time_varying_from6`  
  where variable like "Neuromuscular blocker"
) as neuro on neuro.stay_id = drug.stay_id and neuro.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable in ("Min Riker")
) as min_riker on min_riker.stay_id = drug.stay_id and drug.time_interval - 1 = min_riker.time_interval
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable in ("Max Riker")
) as max_riker on max_riker.stay_id = drug.stay_id and drug.time_interval - 1 = max_riker.time_interval
left join (
  select stay_id, time_interval, variable, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable in ("Propofol", "Ketamine", "Dexmedetomidine")
) as propofol on propofol.stay_id = drug.stay_id and propofol.time_interval = drug.time_interval - 1
left join (
  select stay_id, time_interval, variable, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable in ("Lorazepam", "Diazepam", "Midazolam")
) as benzo on benzo.stay_id = drug.stay_id and benzo.time_interval = drug.time_interval - 1
left join (
	select stay_id, time_interval, variable, value
	from `roses-0.roses.mimic_time_varying_from6`
	where variable like "Min Riker Goal"
) as min_riker_goal on min_riker_goal.stay_id = drug.stay_id and min_riker_goal.time_interval = drug.time_interval - 1
where drug.time_interval >= 0
order by stay_id, time_interval 
),

second_pass as
(select distinct stay_id, time_interval, 
case 
  when drug_dose is not null then drug_dose
  else 0 
end as drug_dose, female, age, height, weight, admit_year, dementia, tbi, sud, white, black, asian, hispanic, cardiac, med_surg, medicare, medicaid, english, heart_rate, resp_rate, 
case
  when fio2 is not null then fio2
  when fio2 is null and time_interval = 0 then first_value(fio2_time_val) over (partition by stay_id order by fio2_time_time desc)
end as fio2, spo2, dbp, sbp, haloperidol, 
case 
  when pao2 is not null then pao2
  when pao2 is null and time_interval = 0 then first_value(pao2_time_val) over (partition by stay_id order by pao2_time_time desc)
end as pao2, case 
  when paco2 is not null then paco2
  when paco2 is null and time_interval = 0 then first_value(paco2_time_val) over (partition by stay_id order by paco2_time_time desc)
end as paco2,
case 
  when gcs is not null then gcs
  when gcs is null and time_interval = 0 then first_value(gcs_time_val) over (partition by stay_id order by gcs_time_time desc)
end as gcs, min_riker, max_riker, min_riker_goal, neuro, max(propofol) over (partition by stay_id, cast(time_interval as int64)) as propofol, max(lorazepam) over (partition by stay_id, cast(time_interval as int64)) as lorazepam, max(ketamine) over (partition by stay_id, cast(time_interval as int64)) as ketamine, max(dexmedetomidine) over (partition by stay_id, cast(time_interval as int64)) as dexmedetomidine, vasopressor, opioid, propofol_24h, benzo_24h
from first_pass)

select stay_id, time_interval, drug_dose, female, age, height, weight, admit_year, dementia, tbi, sud, white, black, asian, hispanic, cardiac, med_surg, medicare, medicaid, english, heart_rate, resp_rate, fio2, spo2, dbp, sbp, haloperidol, pao2, paco2, gcs, min_riker, max_riker, min_riker_goal, neuro, propofol, lorazepam, ketamine, dexmedetomidine, sum(vasopressor) over (partition by stay_id, cast(time_interval as int64)) as vasopressor, sum(opioid) over (partition by stay_id, cast(time_interval as int64)) as opioid, propofol_24h, benzo_24h
from second_pass

order by 1, 2
