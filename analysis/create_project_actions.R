#### load required packages ####
library('tidyverse')
library('yaml')
library('here')
library('glue')


#### create defaults list ####
defaults_list <- list(
  version = "3.0",
  expectations= list(population_size = 150000L)
)


#### create comment and generic action functions ####
# create comment function
comment <- function(...){
  list_comments <- list(...)
  comments <- map(list_comments, ~paste0("## ", ., " ##"))
  comments
}

# create function to convert comment "actions" in a yaml string into proper comments
convert_comment_actions <-function(yaml.txt){
  yaml.txt %>%
    str_replace_all("\\\n(\\s*)\\'\\'\\:(\\s*)\\'", "\n\\1")  %>%
    str_replace_all("([^\\'])\\\n(\\s*)\\#\\#", "\\1\n\n\\2\\#\\#") %>%
    str_replace_all("\\#\\#\\'\\\n", "\n")
}

# create generic action function
action <- function(
    name,
    run,
    dummy_data_file=NULL,
    arguments=NULL,
    needs=NULL,
    highly_sensitive=NULL,
    moderately_sensitive=NULL
){
  
  outputs <- list(
    moderately_sensitive = moderately_sensitive,
    highly_sensitive = highly_sensitive
  )
  outputs[sapply(outputs, is.null)] <- NULL
  
  action <- list(
    run = paste(c(run, arguments), collapse=" "),
    dummy_data_file = dummy_data_file,
    needs = needs,
    outputs = outputs
  )
  action[sapply(action, is.null)] <- NULL
  
  action_list <- list(name = action)
  names(action_list) <- name
  
  action_list
}


#### combine all actions into a list ####
# create list of actions
actions_list <- splice(
  
  # Post YAML disclaimer
  comment("# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #",
          "DO NOT EDIT project.yaml DIRECTLY",
          "This file is created by create_project_actions.R",
          "Edit and run create_project_actions.R to update the project.yaml",
          "# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #"
  ),

  action(
    name = "generate_study_population",
    run = "ehrql:v1 generate-dataset analysis/dataset_definition.py 
    --output output/study_data.csv",
    needs = NULL,
    highly_sensitive = list(
      cohort = glue("output/study_data.csv")
    )
  )
)


#### combine actions_list and defaults_list ####
# combine everything
project_list <- splice(
  defaults_list,
  list(actions = actions_list)
)


#### convert list to yaml and output yaml file ####
# convert list to yaml and output yaml file
as.yaml(project_list, indent=2) %>%
  # convert comment actions to comments
  convert_comment_actions() %>%
  # add one blank line before level 1 and level 2 keys
  str_replace_all("\\\n(\\w)", "\n\n\\1") %>%
  str_replace_all("\\\n\\s\\s(\\w)", "\n\n  \\1") %>%
  writeLines("project.yaml")
print("YAML file printed!")

# return number of actions
print(paste0("YAML created with ",length(actions_list)," actions."))
