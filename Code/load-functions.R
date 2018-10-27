"""
Custome functions to perform feature engineering
"""

suppressPackageStartupMessages({
  library(readr)
  library(tidyverse)
  library(feather)
  library(mlr)
  library(caret)
  library(parallel)
  library(broom)
  library(multidplyr)
  library(doParallel)
})

#======================================================#
# Function to read csv file without printing messages
#======================================================#

read_csv_ <- function(...) suppressMessages(readr::read_csv(...))

#======================================================#
# Function to remove correlated variables from the dataset
#======================================================#

removeCorrelatedVariables <- function(x, corr, except = "") {
  
  if(abs(corr > 1)) stop('Invalid value for cutoff')
  if(corr == 1) stop('No Columns will be removed; choose a different cutoff value')
  if(corr == -1) stop('All columns will be removed; choose a different cutoff value')
  
  corr_mat <- cor(x, use = "complete.obs")
  corr_mat[is.na(corr_mat)] <- 0
  
  col_to_remove <- caret::findCorrelation(corr_mat, cutoff = corr)
  col_names_rmv <- names(x)[col_to_remove]
  col_names_rmv <- col_names_rmv[!col_names_rmv %in% except]
  
  message(paste("Removing columns with high corr:", paste(col_names_rmv, collapse = ',')))
  y <- x %>% dplyr::select(-one_of(col_names_rmv))
  return(y)
  
}

#======================================================#
# Aggregation functions for feature engineering
#======================================================#

Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

num_nas <- function(x) sum(is.na(x))

agg <- funs(mean(., na.rm = TRUE), sd(., na.rm = TRUE), 
            min(., na.rm = TRUE), max(., na.rm = TRUE), 
            sum(., na.rm = TRUE), n_distinct(., na.rm = TRUE),
            Mode(.),
            first(.),
            last(.),
            num_nas)

agg_num <- funs(mean(., na.rm = TRUE), sd(., na.rm = TRUE), 
            min(., na.rm = TRUE), max(., na.rm = TRUE), 
            sum(., na.rm = TRUE),
            Mode(.),
            first(.),
            last(.),
            num_nas)

agg_rec <- funs(mean, min, max, sum, sd, .args = list(na.rm = TRUE))

#======================================================#
# Function to add trend values for variables for each SK_ID_CURR
#======================================================#

add_trend_features <- function(df, rec_feat, feature_names, periods = "all") {
  
  feat_df = data.frame(SK_ID_CURR = unique(df$SK_ID_CURR))
  
  df1 <- df %>% 
    arrange(SK_ID_CURR, (!!sym(rec_feat))) %>% 
    group_by(SK_ID_CURR) %>% 
    mutate(timeline = 1:n()) %>% 
    ungroup()
  
  for(period in periods) {
    
    if(period == "all") {df2 <- df1} else {
      
      df2 <- df1 %>% 
        group_by(SK_ID_CURR) %>% 
        mutate(period_flag = as.integer(sum(timeline == period))) %>% 
        ungroup %>% 
        filter(period_flag == 1L, timeline <= period) 
        
    }
    
    if(nrow(df2) == 0) break
    
    for(feature_name in feature_names) {
      
      df3 <- df2 %>% 
        filter(!is.na((!!sym(feature_name)))) %>% 
        group_by(SK_ID_CURR) %>% 
        nest() %>% 
        mutate(estimated_model = map(data, ~lm(as.formula(paste0(feature_name, "~ -1 + timeline")), data = .))) %>% 
        mutate(estimated_coef = map(estimated_model,
                                    ~ tidy(., conf.int = FALSE))) %>% 
        unnest(estimated_coef) %>%
        select(SK_ID_CURR, estimate) 
      
      names(df3)[2] <- paste("trend", feature_name, period, sep = '_')
      
      message(paste('Trend features for period', period, 'have been created for the feature', feature_name,'...'))
      
      
      feat_df <- feat_df %>% 
        left_join(df3, by = "SK_ID_CURR")
      
      rm(df3); gc()
    }
    
    #if(nrow(df2 == 0)) break
  }
  
  return(feat_df)
  
}

#======================================================#
# Function to perform aggregation functions over recent n values for a feature
#======================================================#

rec_n_summary <- function(df, rec_feat, feature_names, num_rec) {
  
 
  replace_large_NA <- function(x) {
    if_else(x == 365243, NA, x)
  }
  
  orig_df <- data.frame(SK_ID_CURR = unique(df$SK_ID_CURR))
  
  for(rec_n in num_rec) {
  
    df1 <- df %>% 
      arrange(SK_ID_CURR, (!!sym(rec_feat))) %>% 
      group_by(SK_ID_CURR) %>% 
      mutate(rownums = row_number()) %>% 
      filter(rownums <= rec_n) %>% 
      ungroup() %>% 
      select(c("SK_ID_CURR", feature_names)) %>% 
      mutate_at(feature_names, funs(ifelse(. == 365243, NA, .))) %>% 
      mutate_if(is.character, funs(factor(.) %>% as.integer)) %>% 
      group_by(SK_ID_CURR) %>% 
      summarize_all(.funs = agg_rec) %>% 
      removeConstantFeatures() %>% 
      removeCorrelatedVariables(0.95, except = "SK_ID_CURR") #%>% 
      #rename_at(.vars = vars(-SK_ID_CURR), funs(paste("rec", rec_n, names(.), sep = "_")))
    
      names(df1)[-1] <- paste("rec", rec_n, names(df1)[-1], sep = "_")
    
    orig_df <- orig_df %>% 
      left_join(df1, by = "SK_ID_CURR")
  }
  
  return(orig_df)
}