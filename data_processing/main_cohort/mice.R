library('mice')
library(data.table)
library(tidyverse)
library(magrittr)
library(tidyr)
library(brms)
library('dplyr')
library(ggplot2)
library(patchwork)
library(posterior)

# You may have to set working directory with setwd()
args<-commandArgs(TRUE)
file <- args[1]
outcome_file <- args[2]

logit <- function(x){log(x/(1-x))}
inv_logit <- function(x){exp(x)/(1+exp(x))}

ln_vars <- c(
  "pao2",
  "paco2",
  "heart_rate",
  "resp_rate",
  "sbp",
  "dbp"
)

all_vars <- c(
  "pao2",
  "paco2",
  "heart_rate",
  "resp_rate",
  "sbp",
  "dbp",
  "fio2",
  "gcs",
  "spo2",
  "min_riker",
  "max_riker"
)

transform <- function(col, ln_vars, all_vars){ 
  variable <- col[1]
  value <- as.numeric(col[-1])
  if (variable %in% all_vars) {
    value <- value+2.220446049250313e-16
  }
  
  if(variable %in% ln_vars){
    log(value)
  }
  # use logit transformation functions for GCS, spo2, and fio2
  # because they have a restricted range 
  else if (variable == "gcs"){
    logit((value-2.9)/12.2)}
  else if (variable == "spo2"){
    logit(value/101)} # take the floor when transforming back
  else if (variable == "fio2"){
    logit((value-20.9)/79.3)} # take the floor when transforming back
  else if (variable == "min_riker" || variable == 'max_riker') {
    logit((value-0.9)/6.2)
  }
  else value
}

data <- fread(file)

print(data[data$stay_id == 32442329])

data <- data %>% mutate(propofol_outcome_divided = ifelse(is.na(weight), NA, propofol_outcome/weight),
                        benzo_outcome_divided = ifelse(is.na(weight), NA, benzo_outcome/weight),
                        dexmedetomidine_outcome_divided = ifelse(is.na(weight), NA, dexmedetomidine_outcome/weight))

test <- apply(select(data, propofol_outcome_divided, benzo_outcome_divided, dexmedetomidine_outcome_divided), 2, 
              function(col, weight){
                not_zero <- col[col!=0]
                q <- quantile(not_zero, na.rm=TRUE)
                return(q)
              }, weight=data$weight)

saveRDS(test, file=paste0('quantiles/outcome_quantile.RDS'))
q <- readRDS('quantiles/outcome_quantile.RDS')

data <- select(data, -ketamine_outcome)

col_name <- c("stay_id","time_interval","female","age","height","weight","admit_year","dementia","tbi", "sud", "white", "black", "asian", "hispanic","cardiac","med_surg","medicare", "medicaid", "english", "heart_rate", "resp_rate", "fio2", "spo2", "sbp","dbp", "haloperidol", "pao2", "paco2", "gcs", "min_riker", "max_riker", "min_riker_goal", "neuro", "propofol", "lorazepam", "ketamine", "dexmedetomidine", "vasopressor", "opioid", "propofol_24h", "benzo_24h", "propofol_outcome", "benzo_outcome", "dexmedetomidine_outcome", "min_riker_outcome", "propofol_outcome_divided", "benzo_outcome_divided","dexmedetomidine_outcome_divided")

a <- data.table("stay_id","time_interval","female","age","height","weight","admit_year","dementia","tbi", "sud", "white", "black", "asian", "hispanic","cardiac","med_surg","medicare", "medicaid", "english", "heart_rate", "resp_rate", "fio2", "spo2", "sbp","dbp","haloperidol","pao2","paco2","gcs","min_riker", "max_riker", "min_riker_goal", "neuro", "propofol", "lorazepam", "ketamine", "dexmedetomidine", "vasopressor", "opioid", "propofol_24h", "benzo_24h", "propofol_outcome", "benzo_outcome", "dexmedetomidine_outcome", "min_riker_outcome", "propofol_outcome_divided", "benzo_outcome_divided","dexmedetomidine_outcome_divided")
names(a) <- col_name
test <- rbind(a, data)

# TRANSFORM

transformed_data <- test
transformed_data <- apply(transformed_data, 2, transform, ln_vars, all_vars)
transformed_data <- as.data.table(unname(transformed_data))
names(transformed_data) <- col_name

# center & scale 
data <- transformed_data
mean_sd <- data.frame()
mean_sd[1,]=NA  # add temporary row/cols to make the matrix size cooperate
mean_sd[2,]=NA  
mean_sd[,'new_column'] = NA
for (i in all_vars){
  mean_sd <- cbind(mean_sd, 
                   c(mean(transformed_data[!is.na(transformed_data[[i]]) & transformed_data[[i]] > -Inf,][[i]]), 
                     sd(transformed_data[!is.na(transformed_data[[i]]) & transformed_data[[i]] > -Inf,][[i]])))
}
mean_sd <- subset(mean_sd, select = -c(1)) # drop fake col
colnames(mean_sd) <- all_vars
saveRDS(mean_sd, file = 'quantiles/mean_sd.RDS')

for (i in all_vars){
  data[[i]] <-  
    (data[[i]] - mean_sd[1, i])/mean_sd[2, i]
}

# MICE
# ignore certain columns
M <- 1 - diag(length(col_name))
for (i in c(1, 2, 3, 4, 12, 13, 14, 15, 16, 17, 18, 19, 20, 27, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48)) {
  M[, i] <- 0
}

# impute
imp <- mice(filter(data, time_interval == 0), method = "cart", predictorMatrix = M, m=10)
first_dataset <- complete(imp, 1)

# rejoin imputed data
all_data <- rbind(first_dataset, filter(data, time_interval > 0)) 
# rejoin imputed data from all datasets
indices <- c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
complete_data <- lapply(indices, function(i) {
  temp_data <- complete(imp, i)
  temp_data <- rbind(temp_data, filter(data, time_interval > 0))
  
})

# categorize drug quantiles
q <- readRDS('quantiles/outcome_quantile.RDS')
all_data <- all_data %>% mutate(
  propofol_outcome_quantile = case_when(
    propofol_outcome_divided <= 0 ~ 1, 
    propofol_outcome_divided <= q[[2,1]] ~ 2,
    propofol_outcome_divided <= q[[3,1]] ~ 3,
    propofol_outcome_divided <= q[[4,1]] ~ 4,
    propofol_outcome_divided > q[[4,1]] ~ 5
  )
)

q <- readRDS('quantiles/outcome_quantile.RDS')
all_data <- all_data %>% mutate(
  benzo_outcome_quantile = case_when(
    benzo_outcome_divided <= 0 ~ 1, 
    benzo_outcome_divided <= q[[2,2]] ~ 2,
    benzo_outcome_divided <= q[[3,2]] ~ 3,
    benzo_outcome_divided <= q[[4,2]] ~ 4,
    benzo_outcome_divided > q[[4,2]] ~ 5
  )
)

complete_data <- lapply(complete_data, function (df) {
  q2 <- readRDS('quantiles/outcome_quantile.RDS')
  df2 <- df %>% mutate(
    propofol_outcome_quantile = case_when(
      propofol_outcome_divided <= 0 ~ 1, 
      propofol_outcome_divided <= q2[[2,1]] ~ 2,
      propofol_outcome_divided <= q2[[3,1]] ~ 3,
      propofol_outcome_divided <= q2[[4,1]] ~ 4,
      propofol_outcome_divided > q2[[4,1]] ~ 5
    )
  )
  
  q2 <- readRDS('quantiles/outcome_quantile.RDS')
  df2 <- df2 %>% mutate(
    benzo_outcome_quantile = case_when(
      benzo_outcome_divided <= 0 ~ 1, 
      benzo_outcome_divided <= q2[[2,2]] ~ 2,
      benzo_outcome_divided <= q2[[3,2]] ~ 3,
      benzo_outcome_divided <= q2[[4,2]] ~ 4,
      benzo_outcome_divided > q2[[4,2]] ~ 5
    )
  )
  
  df2 <- df2 %>% mutate(
    min_riker_outcome = ifelse(min_riker_outcome == 0, NA, min_riker_outcome)
  ) %>% arrange(, stay_id, time_interval) %>%
    group_by(stay_id) %>%
    fill(everything(), .direction = "down") %>%
    ungroup()
  
  return(df2)
})

# adjust complete_data variables

factor_categorical_vars <- function(complete_tables){
  for(i in 1:length(complete_tables)){
    complete_tables[[i]] <- complete_tables[[i]] %>%
      mutate(stay_id = factor(stay_id),
             female = factor(female),
             admit_year = factor(admit_year),
             dementia = factor(dementia),
             white = factor(white),
             black = factor(black),
             asian = factor(asian),
             hispanic = factor(hispanic),
             cardiac = factor(cardiac),
             med_surg = factor(med_surg),
             medicare = factor(medicare),
             medicaid = factor(medicaid),
             neuro = factor(neuro), 
             # is lorazepam the combined benzodiazepines in lorazepam equivalents?
             propofol_outcome_quantile = factor(propofol_outcome_quantile, 
                                                levels = 1:5, ordered = T),
             benzo_outcome_quantile = factor(benzo_outcome_quantile, 
                                             levels = 1:5, ordered = T), 
             age_less_55 = factor(ifelse(age < 55, 1, 0)), 
             age_55_65 = factor(ifelse(age >= 55 & age < 65, 1, 0)), 
             age_65_75 = factor(ifelse(age >= 65 & age < 75, 1, 0)), 
             age_greater_75 = factor(ifelse(age >= 75, 1, 0)), 
             day_1 = factor(ifelse(time_interval < 6, 1, 0)), 
             day_2 = 
               factor(ifelse(time_interval >= 6 & time_interval < 12, 1, 0)), 
             day_3 = 
               factor(ifelse(time_interval >= 12 & time_interval < 18, 1, 0)), 
             day_4 = 
               factor(ifelse(time_interval >= 18 & time_interval < 24, 1, 0)), 
             day_5 = 
               factor(ifelse(time_interval >= 24 & time_interval < 30, 1, 0)), 
             day_6 = 
               factor(ifelse(time_interval >= 30 & time_interval < 36, 1, 0)), 
             day_7 = 
               factor(ifelse(time_interval >= 36 & time_interval < 42, 1, 0)), 
             dexmedetomidine_outcome = 
               factor(ifelse(dexmedetomidine_outcome != 0, 1, 0))
      ) %>%
      filter(white == 1 | black == 1 | asian == 1 | hispanic == 1) %>%
      select(-white) %>% # switch to white reference case
      filter(time_interval != 42) %>% # since starts at time interval = 0
      mutate(propofol = propofol/(weight*240*10), 
             dexmedetomidine = 5*dexmedetomidine/(weight*4),
             benzo = 10*lorazepam/(weight*4*1000), # rename
             vasopressor = 10*vasopressor/(weight*240), # 0.1mcg/kg/min
             opioid = opioid/(weight*1000*4), 
             propofol_24h = propofol_24h/(weight*24*60*10), 
             benzo_24h = 10*benzo_24h/(weight*24*1000), 
      ) %>% mutate (
        nmb = neuro, # rename
        vasopressor = ifelse(vasopressor > 10, 10, vasopressor), 
        opioid = ifelse(opioid > 1, 1, opioid)
      )
  }
  complete_tables
}

complete_data <- factor_categorical_vars(complete_data)

saveRDS(complete_data, outcome_file)

# forward fill, save, & return 1 dataset
# fill <- all_data %>% arrange(, stay_id, time_interval) %>%
#   group_by(stay_id) %>%
#   fill(everything(), .direction = "down") %>%
#   ungroup()
# saveRDS(fill, paste0('../results/r/table.RDS'))
