rm(list=ls())
sink("/data/logs/run_ga_sink.txt")
logEntry <- paste("running run_ga.R at",Sys.time())
try(write(logEntry,"/data/logs/log.txt",append = T))

##################
# SETUP
#################

source("/home/user/scripts/libs.R")
source("/home/user/scripts/ga/api.R")
source("/home/user/scripts/system/ftp.R")
source("/home/user/scripts/system/tools.R")

##################
# INITIALIZE
#################

#initialze mapp tools
tools <- mappMarketingTools$new()
#initialize ftp storage api
f <- ftp$new(name="ecircle_marketing",settings=settings)
#initialize google analytics api wrapper
g <- googleAnalytics$new(settings=settings)
#set from to yesterday
g$from <- as.character(Sys.Date()-1)
#set metrics and dimensions
g$metrics = "ga:sessions,ga:pageviews,ga:avgSessionDuration,ga:bounces,ga:goal2Completions,ga:goal8Completions,ga:goal9Completions"
g$dimensions = "ga:date,ga:fullreferrer,ga:country,ga:landingPagePath,ga:medium,ga:userType,ga:source"
#edit filter on  'mapp' property and remove other properties
g$properties[[3]]['segment'] <- "gaid::-1"
g$properties[c(1,2,4)]<-NULL

##################
# DATA
##################

#get data
g$data$mapp <- try(g$getData())

if(!inherits(g$data$mapp,"try-error")){
  
  #filter out problematic expressions
  g$data$mapp <- g$data$mapp[!grepl("<%${",g$data$mapp$landingPagePath,fixed=T),]
  g$data$mapp <- g$data$mapp[!grepl("flagship",g$data$mapp$fullreferrer,fixed=T),]
  
  #compute origin variables
  g$data$mapp$origin <- apply(g$data$mapp,1,g$getTrafficSource)
  g$data$mapp$target <- apply(g$data$mapp,1,g$getTrafficDestination)
  #categorize inbound and outbound variables
  g$data$mapp$inbound <- "Outbound"
  g$data$mapp$inbound[g$data$mapp$origin %in% c("direct","organic","social","blogs","own_domains","referral")]<-"Inbound"
  #edit url for later merging
  g$data$mapp$url <- paste0("https://mapp.com",str_replace_all(g$data$mapp$landingPage,"mapp\\.com",""))
  g$data$mapp$area <-sapply(g$data$mapp$country,tools$mapGeo,get="region")
  #save resulting dataset to the ga instance slot
  g$saveData(slot = "mapp",filename = paste0("/data/ga/",Sys.Date(),"gadata.rds"))

##################
# BIND
##################

#collect all 'gadata' files
gaDataFiles <- list.files("/data/ga","gadata",full.names=T)
#bind them
gaDataLists <- lapply(gaDataFiles,readRDS)
gaData <- do.call("rbind",gaDataLists)

##################
# SAVE
##################

#save to folder
saveRDS(gaData,"/data/ga/data.rds")
#dump to ftp
f$dump(gaData,"gaData.csv")

rm(list=ls())
sink() 
}


