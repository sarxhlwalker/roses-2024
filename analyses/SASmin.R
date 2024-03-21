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
    filter(!is.na(min_riker_outcome)) %>% 
    mutate(min_riker_outcome = factor(min_riker_outcome, ordered = T, labels = 1:7)) %>%
    mutate(min_riker_outcome = ifelse(min_riker_outcome > 5, 5, min_riker_outcome)) %>%
    mutate(sas = factor(as.character(min_riker_outcome), ordered = T, labels=1:5))

}

priors <- c(prior(normal(0, 0.3), class=b), 
            prior(normal(0, 0.3), class=sd), 
            prior(normal(-1, 1), class=Intercept, coef=1),
	    prior(normal(0, 1), class=Intercept, coef=2), 
	    prior(normal(2, 1), class=Intercept, coef=3), 
	    prior(normal(4, 1), class=Intercept, coef=4))


if (i == 1) {
	sas_singlefit <- brm(sas ~ 
                asian + black + hispanic 
                + female 
                + age_55_65 + age_65_75 + age_greater_75
                + dementia + tbi + sud 
                + english + medicare + medicaid
                + cardiac + med_surg + admit_year 
                + (1 | stay_id)  
                + fio2 + spo2 
                + day2_3 + day4_7 + nmb, 
                    data=complete_tables[[i]], 
                    family=cumulative(), iter=2000, 
                    prior=priors, threads=threading(80/(4*n_parallel)), chains = 4,
                    cores = 80/n_parallel,
                    backend = "cmdstanr",
                    save_pars = save_pars(group = FALSE),
                    init = 0,
                   refresh = 10)
        
	saveRDS(sas_singlefit, file = paste0("output/sas_singlefit", i, ".rds"))
} else if (i == 2) {
	five <- complete_tables[1:5]
	sas_five <- brm_multiple(sas ~ 
                asian + black + hispanic 
                + female 
                + age_55_65 + age_65_75 + age_greater_75
                + dementia + tbi + sud 
                + english + medicare + medicaid
                + cardiac + med_surg + admit_year 
                + (1 | stay_id)  
                + fio2 + spo2 
                + day2_3 + day4_7 + nmb, 
                    data=five, 
                    family=cumulative(), iter=2000, 
                    prior=priors, threads=threading(80/(4*n_parallel)), chains = 4,
                    cores = 80/n_parallel,
                    backend = "cmdstanr",
                    save_pars = save_pars(group = FALSE),
                    init = 0,
                   refresh = 10)
        
	saveRDS(sas_five, file = "output/sas_five.rds")
}
