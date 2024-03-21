with first_pass as (
select baseline.stay_id, baseline.hadm_id, baseline.subject_id, heart_rate, resp_rate, fio2.fio2, fio2.time_interval as fio2_time, spo2.spo2, sbp.sbp, dbp.dbp,
case when haloperidol.haloperidol is not null then haloperidol.haloperidol
  else 0
end as haloperidol,
case when meperidine.value is not null then meperidine.value
  else 0
end as meperidine,
case when fentanyl.fentanyl is not null then fentanyl.fentanyl
  else 0
end as fentanyl,
case when hydromorphone.value is not null then hydromorphone.value
  else 0
end as hydromorphone,
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
  when eye.value is not null and motor.value is not null and verbal.value is not null
    and eye.time_interval = motor.time_interval and motor.time_interval = verbal.time_interval
    then eye.value + motor.value + verbal.value
  else null
end as gcs,
case
  when eye.value is not null and motor.value is not null and verbal.value is not null
    and eye.time_interval = motor.time_interval and motor.time_interval = verbal.time_interval
    then eye.time_interval
  else null
end as gcs_time,
pao2.value as pao2,
pao2.time_interval as pao2_time,
paco2.value as paco2,
paco2.time_interval as paco2_time,
case
  when neuro.value is not null then neuro.value
  else 0
end as neuro,
min_riker.value as min_riker,
max_riker.value as max_riker,
case when propofol.value is not null and propofol.variable like "Propofol" then propofol.value else 0 end as propofol,
case when propofol.value is not null and propofol.variable like "Ketamine" then propofol.value else 0 end as ketamine,
case when propofol.value is not null and propofol.variable like "Dexmedetomidine" then propofol.value else 0 end as dexmedetomidine,
case
  when benzo.variable like "Lorazepam" then benzo.value
  when benzo.variable like "Diazepam" then benzo.value * (1/5)
  when benzo.variable like "Midazolam" then benzo.value * (1/2)
  else 0
end as lorazepam
from `roses-0.roses.baseline_mimic_24h` as baseline
left join (
  select stay_id, time_interval, value as heart_rate
  from `roses-0.roses.mimic_time_varying_from6`
    where variable like "heart_rate"
) as heart on heart.stay_id = baseline.stay_id and heart.time_interval = -1
left join (
  select stay_id, time_interval, value as resp_rate
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "resp_rate"
) as resp on resp.stay_id = baseline.stay_id and resp.time_interval = -1
left join (
  select stay_id, time_interval, value as fio2
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "inspired oxygen fraction"
) as fio2 on fio2.stay_id = baseline.stay_id and fio2.time_interval < 0
left join (
  select stay_id, time_interval, value as spo2
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "peripheral oxygen saturation"
) as spo2 on spo2.stay_id = baseline.stay_id and spo2.time_interval = -1
left join (
  select stay_id, time_interval, value as sbp
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "systolic blood pressure"
) as sbp on sbp.stay_id = baseline.stay_id and sbp.time_interval = -1
left join (
  select stay_id, time_interval, value as dbp
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "diastolic blood pressure"
) as dbp on dbp.stay_id = baseline.stay_id and dbp.time_interval = -1
left join (
  select stay_id, time_interval, value as haloperidol
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Haloperidol"
) as haloperidol on haloperidol.stay_id = baseline.stay_id and haloperidol.time_interval = -1
left join (
  select stay_id, time_interval, value as fentanyl
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Fentanyl"
) as fentanyl on fentanyl.stay_id = baseline.stay_id and fentanyl.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Hydromorphone"
) as hydromorphone on hydromorphone.stay_id = baseline.stay_id and hydromorphone.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Morphine"
) as morphine on morphine.stay_id = baseline.stay_id and morphine.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Meperidine"
) as meperidine on meperidine.stay_id = baseline.stay_id and meperidine.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Norepinephrine"
) as norepinephrine on norepinephrine.stay_id = baseline.stay_id and norepinephrine.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Dopamine"
) as dopamine on dopamine.stay_id = baseline.stay_id and dopamine.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Vasopressin"
) as vasopressin on vasopressin.stay_id = baseline.stay_id and vasopressin.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Epinephrine"
) as epinephrine on epinephrine.stay_id = baseline.stay_id and epinephrine.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Dobutamine"
) as dobutamine on dobutamine.stay_id = baseline.stay_id and dobutamine.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Milrinone"
) as milrinone on milrinone.stay_id = baseline.stay_id and milrinone.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "GCS - eye"
) as eye on eye.stay_id = baseline.stay_id and eye.time_interval < 0
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "GCS - verbal"
) as verbal on verbal.stay_id = baseline.stay_id and verbal.time_interval < 0
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "GCS - motor"
) as motor on motor.stay_id = baseline.stay_id and motor.time_interval < 0
left join (
  select stay_id, time_interval, value
from `roses-0.roses.mimic_time_varying_from6`
  where variable like "PaO2"
) as pao2 on pao2.stay_id = baseline.stay_id and pao2.time_interval < 0
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "PaCO2"
) as paco2 on paco2.stay_id = baseline.stay_id and paco2.time_interval < 0
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable like "Neuromuscular blocker"
) as neuro on neuro.stay_id = baseline.stay_id and neuro.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable in ("Min Riker")
) as min_riker on min_riker.stay_id = baseline.stay_id and min_riker.time_interval = -1
left join (
  select stay_id, time_interval, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable in ("Max Riker")
) as max_riker on max_riker.stay_id = baseline.stay_id and max_riker.time_interval = -1
left join (
  select stay_id, time_interval, variable, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable in ("Propofol", "Ketamine", "Dexmedetomidine")
) as propofol on propofol.stay_id = baseline.stay_id and propofol.time_interval = -1
left join (
  select stay_id, time_interval, variable, value
  from `roses-0.roses.mimic_time_varying_from6`
  where variable in ("Lorazepam", "Diazepam", "Midazolam")
) as benzo on benzo.stay_id = baseline.stay_id and benzo.time_interval = -1
)
,

second_pass as (
  select distinct first_pass.stay_id, hadm_id, subject_id, heart_rate, resp_rate, first_value(fio2) over (partition by first_pass.stay_id order by fio2_time desc) as fio2, spo2, dbp, sbp, haloperidol, fentanyl, hydromorphone, morphine, meperidine, norepinephrine, epinephrine, dopamine, vasopressin, dobutamine, milrinone, first_value(pao2) over (partition by first_pass.stay_id order by pao2_time desc) as pao2, first_value(paco2) over (partition by first_pass.stay_id order by paco2_time desc) as paco2, first_value(gcs) over (partition by first_pass.stay_id order by gcs_time desc) as gcs, min_riker, max_riker, neuro, max(propofol) over (partition by first_pass.stay_id) as propofol, max(lorazepam) over (partition by first_pass.stay_id) as lorazepam, max(ketamine) over (partition by first_pass.stay_id) as ketamine, max(dexmedetomidine) over (partition by first_pass.stay_id) as dexmedetomidine, vasopressor, opioid
from first_pass
), 

third_pass as (
select distinct second_pass.stay_id, hadm_id, subject_id, heart_rate, resp_rate, fio2, spo2, dbp, sbp, haloperidol, fentanyl, hydromorphone, morphine, meperidine, norepinephrine, epinephrine, dopamine, vasopressin, dobutamine, milrinone, pao2, paco2, gcs, min_riker, max_riker, neuro, propofol, lorazepam, ketamine, dexmedetomidine, vasopressor, opioid,
case
  when sum(prop.value) over (partition by second_pass.stay_id) is not null then sum(prop.value) over (partition by second_pass.stay_id)
  else 0
end as propofol_24h,
case
  when sum(benzo.value) over (partition by second_pass.stay_id) is not null then sum(benzo.value) over (partition by second_pass.stay_id)
  else 0
end as benzo_24h
from second_pass
left join `roses-0.roses.mimic_time_varying_from6` as prop on prop.stay_id = second_pass.stay_id and prop.time_interval < 0 and prop.variable in ("Propofol")
left join (
        select time.stay_id, time.time_interval, "benzo" as variable,
        case
                when time.variable like "Diazepam" then time.value / 5
                when time.variable like "Midazolam" then time.value / 2
                else time.value
        end as value,
        from `roses-0.roses.mimic_time_varying_from6` as time
        where time.variable in ("Lorazepam", "Diazepam", "Midazolam") and time.time_interval < 0
) as benzo on benzo.stay_id = second_pass.stay_id
)

select stay_id, hadm_id, subject_id, heart_rate, resp_rate, fio2, spo2, dbp, sbp, haloperidol, fentanyl, hydromorphone, morphine, meperidine, norepinephrine, epinephrine, dopamine, vasopressin, dobutamine, milrinone, pao2, paco2, gcs, min_riker, max_riker, neuro, propofol, lorazepam, ketamine, dexmedetomidine, propofol_24h, benzo_24h, sum(vasopressor) over (partition by stay_id) as vasopressor, sum(opioid) over (partition by stay_id) as opioid
from third_pass
