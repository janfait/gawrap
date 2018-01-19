#################
# SETUP
#################

source("/home/user/scripts/libs.R")
source("/home/user/scripts/ga/search_api.R")
options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/webmasters") 

#################
# INITIALIZE
#################

sc <- searchConsole$new(
  auth="/home/user/settings/search_console_key.json",
  property="https://mapp.com"
)

gar_auth_service("/home/user/settings/search_console_key.json")

list_websites()

#################
# DATA
#################

data <- sc$getData(
  from=as.character(Sys.Date()-28),
  to=as.character(Sys.Date()-5),
  splits="day",
  dimensions=c("date","query","country")
)

#################
# SAVE
#################

sc$saveData(filename = paste0(sc$paths$store,"/",Sys.Date(),"search.rds"))

##############
# BIND
##############

scDataFiles <- list.files("/data/ga","search",full.names=T)
scDataLists <- lapply(scDataFiles,readRDS)
scDataLists <- lapply(scDataLists,function(l){
  if(length(colnames(l))<10){
    l <- sc$makeDateVars(data = l)
  }
  return(l)
})

scData <- do.call("rbind",scDataLists)
scData <- unique(scData)

##############
# SAVE
##############

try(saveRDS(scData,"/data/ga/search.rds")) 

