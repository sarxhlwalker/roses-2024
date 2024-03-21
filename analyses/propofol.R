library(brms)
library(dplyr)

args<-commandArgs(TRUE)

i <- as.numeric(args[1])
n_parallel <- as.numeric(args[2])

complete_tables <- readRDS("data/complete_tables4.rds") 

for(m in 1:10){
    complete_tables[[m]] <- complete_tables[[m]] %>%
    mutate(day2_3 = case_when(day_2 == "1" ~ 1,
                              day_3 == "1" ~ 1,
                              TRUE ~ 0),
           day4_7 = case_when(day_4 == "1" ~ 1,
                              day_5 == "1" ~ 1,
                              day_6 == "1" ~ 1,
                              day_7 == "1" ~ 1,
                              TRUE ~ 0)) %>%
	mutate(min_riker = factor(min_riker, ordered = T, labels=1:7)) %>% 
	mutate(min_riker = ifelse(min_riker > 5, 5, min_riker)) %>% 
	mutate(min_riker = factor(as.character(min_riker), ordered = T, labels=1:5)) %>% 
	mutate(
		unarousable = factor(ifelse(min_riker == 1, 1, 0)), 
		very_sedated = factor(ifelse(min_riker == 2, 1, 0)), 
		sedated = factor(ifelse(min_riker == 3, 1, 0)), 
		calm = factor(ifelse(min_riker == 4, 1, 0)), 
		agitated = factor(ifelse(min_riker == 5, 1, 0))
	)
}

priors <- c(prior(normal(0, 0.3), class=b), 
            prior(normal(0, 0.3), class=sd), 
            prior(normal(2, 1), class=Intercept, coef = 1),
            prior(normal(4, 1), class=Intercept, coef=2),
            prior(normal(6, 1), class=Intercept, coef=3),
            prior(normal(8, 1), class=Intercept, coef=4)
)

if(i == 0){
propofol_prior <- brm(propofol_outcome_quantile ~ 
                asian + black + hispanic 
                + female 
                + age_55_65 + age_65_75 + age_greater_75
                + weight + height 
                + dementia + tbi + sud 
                + english + medicare + medicaid
                + cardiac + med_surg + admit_year 
                + propofol_24h + benzo_24h 
                + (1 | stay_id) 
                + heart_rate + resp_rate + fio2 + spo2 + sbp + dbp 
                + gcs + pao2 + paco2 
                + day_2 + day_3 + day_4 + day_5 + day_6 + day_7
                + dexmedetomidine 
                + benzo 
                + opioid 
                + vasopressor, 
                    data=complete_tables[[1]], 
                    family=cumulative(), iter=2000, 
                    prior=priors, chains = 4,
                    cores = 4,
                    backend = "cmdstanr",
                    sample_prior = "only",
                    save_pars = save_pars(group = FALSE),
                    sample_prior = TRUE,
                    init = 0,
                   refresh = 100)

saveRDS(propofol_prior, file = "output/propofol_prior.rds")
} else if (i == 1) { 
	propofol_singlefit <- brm(propofol_outcome_quantile ~ 
                asian + black + hispanic 
                + female 
                + age_55_65 + age_65_75 + age_greater_75
                + dementia + tbi + sud 
                + english + medicare + medicaid
                + cardiac + med_surg + admit_year 
                + propofol_24h + benzo_24h 
                + (1 | stay_id)  
                + resp_rate + fio2 + spo2 
                + day2_3 + day4_7
                + dexmedetomidine 
                + benzo 
                + opioid 
                + vasopressor
		+ nmb, 
                    data=complete_tables[[i]], 
                    family=cumulative(), iter=3000, 
                    prior=priors, threads=threading(80/(4*n_parallel)), chains = 4,
                    cores = 80/n_parallel,
                    backend = "cmdstanr",
                    save_pars = save_pars(group = FALSE),
                    init = 0,
                   refresh = 10, warmup=2000)
        
saveRDS(propofol_singlefit, file = paste0("output/propofol_singlefit", i, "_03_3k_warmup.rds"))
} else if (i == 2) {
	five <- list(complete_tables[[1]], complete_tables[[2]], complete_tables[[3]], 
			complete_tables[[4]], complete_tables[[5]])
	propofol_all <- brm_multiple(propofol_outcome_quantile ~
		asian + black + hispanic + female
                + age_55_65 + age_65_75 + age_greater_75
                + dementia + tbi + sud 
                + english + medicare + medicaid
                + cardiac + med_surg + admit_year 
                + propofol_24h + benzo_24h 
                + (1 | stay_id)  
                + resp_rate + fio2 + spo2 
                + day2_3 + day4_7
                + dexmedetomidine 
                + benzo 
                + opioid 
                + vasopressor
		+ nmb
		+ very_sedated + sedated + calm + agitated,
                    data=five, 
                    family=cumulative(), iter=4000, 
                    prior=priors, threads=threading(80/(4*n_parallel)), chains = 4,
                    cores = 80/n_parallel,
                    backend = "cmdstanr",
                    save_pars = save_pars(group = FALSE),
                    init = 0,
                   refresh = 10, warmup=3000)

saveRDS(propofol_all, file=paste0("output/propofol_five_sas2.rds"))
} else if (i == 3) {
    five <- complete_tables[1:5]
    propofol_interaction <- brm_multiple(propofol_outcome_quantile ~
                asian + black + hispanic
                + female
        + asian*female + black*female + hispanic*female
                + age_55_65 + age_65_75 + age_greater_75
                + dementia + tbi + sud
                + english + medicare + medicaid
                + cardiac + med_surg + admit_year
                + propofol_24h + benzo_24h
                + (1 | stay_id)
                + resp_rate + fio2 + spo2
                + day2_3 + day4_7
                + dexmedetomidine
                + benzo
                + opioid
                + vasopressor
        + nmb
        + very_sedated + sedated + calm + agitated,
                    data=five,
                    family=cumulative(), iter=4000, warmup=3000,
                    prior=priors, threads=threading(80/(4*n_parallel)), chains = 4,
                    cores = 80/n_parallel,
                    backend = "cmdstanr",
                    save_pars = save_pars(group = FALSE),
                    init = 0,
                   refresh = 10)

    saveRDS(propofol_interaction, file="output/propofol_interaction_five_sas2.rds")
} else if (i == 4) {
	no_race <- readRDS("data/complete_tables_no_race.RDS")
	for(m in 1:10){
    		no_race[[m]] <- no_race[[m]] %>%
    		mutate(day2_3 = case_when(day_2 == "1" ~ 1,
                              day_3 == "1" ~ 1,
                              TRUE ~ 0),
           	day4_7 = case_when(day_4 == "1" ~ 1,
                              day_5 == "1" ~ 1,
                              day_6 == "1" ~ 1,
                              day_7 == "1" ~ 1,
                              TRUE ~ 0)) %>%
		mutate(min_riker = factor(min_riker, ordered = T, labels=1:7)) %>% 
		mutate(min_riker = ifelse(min_riker > 5, 5, min_riker)) %>% 
		mutate(min_riker = factor(as.character(min_riker), ordered = T, labels=1:5)) %>% 
		mutate(
			unarousable = factor(ifelse(min_riker == 1, 1, 0)), 
			very_sedated = factor(ifelse(min_riker == 2, 1, 0)), 
			sedated = factor(ifelse(min_riker == 3, 1, 0)), 
			calm = factor(ifelse(min_riker == 4, 1, 0)), 
			agitated = factor(ifelse(min_riker == 5, 1, 0))
		)
	}
	no_race <- no_race[1:5]
	propofol_all <- brm_multiple(propofol_outcome_quantile ~
		asian + black + hispanic + other + female
                + age_55_65 + age_65_75 + age_greater_75
                + dementia + tbi + sud 
                + english + medicare + medicaid
                + cardiac + med_surg + admit_year 
                + propofol_24h + benzo_24h 
                + (1 | stay_id)  
                + resp_rate + fio2 + spo2 
                + day2_3 + day4_7
                + dexmedetomidine 
                + benzo 
                + opioid 
                + vasopressor
		+ nmb
		+ very_sedated + sedated + calm + agitated,
                    data=no_race, 
                    family=cumulative(), iter=4000, 
                    prior=priors, threads=threading(80/(4*n_parallel)), chains = 4,
                    cores = 80/n_parallel,
                    backend = "cmdstanr",
                    save_pars = save_pars(group = FALSE),
                    init = 0,
		refresh = 10)
	saveRDS(propofol_all, file="output/propofol_no_race.rds")
}

