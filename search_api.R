searchConsole <- setRefClass("searchConsole",
    fields = list(
     debug = "logical",
     paths = "list",
     property = "character",
     initializeTime = "POSIXt",
     rounding ="numeric",
     data = "list"
    ),
    methods = list(
     debugPrint = function(a){
       if(.self$debug){
         cat(deparse(substitute(a)),"\n")
         if(is.data.frame(a)){
           str(a)
         }else{
           str(a)
           print(a)
         }
       }
     },
     getData = function(from,to,splits,dimensions,type="web",limit=5000,lazyLoad=F){
       if(lazyLoad){
         out <- readRDS(paste0(.self$paths$store,"search.rds"))
         return(out)
       }
       out <- list()
       splitsFrom <- seq(as.Date(from), as.Date(to), by = splits) 
       splitsTo <- c(splitsFrom[-1],as.Date(to))

       l <- mapply(function(f,t){
         
         d <- try(search_analytics(
           siteURL = .self$property,
           startDate = f, 
           endDate = t, 
           dimensions = dimensions,
           searchType = type, 
           rowLimit = limit)
         )
         if(inherits(d,"try-error")){
           d<-NULL
         }else{
           d<-as.data.frame(d)
         }
         return(d)
       },f=splitsFrom,t=splitsTo,SIMPLIFY=F)
       
       .self$data$list <- l
       expectedLength <- length(dimensions)+1+4
       out <- do.call("rbind",l[sapply(l,length)==expectedLength])
       cols <- colnames(out)
       
       if('date' %in% cols){
         out$week <- lubridate::floor_date(out$date,"week")
         out$month <- lubridate::floor_date(out$date,"month")
         out$year <- lubridate::floor_date(out$date,"year")
         out$month.label <- paste(lubridate::year(out$date),lubridate::month(out$date,label = F),lubridate::month(out$date,label = T),sep="-")
         out$week.label <- paste(lubridate::year(out$date),lubridate::week(out$date),sep="-")
         out$year.label <- lubridate::year(out$date)
       }
       .self$data$latest <- out
       return(out)
     },
     makeDateVars = function(data=NULL){
       out <- data
       cols <- colnames(data)
       if('date' %in% cols){
         out$week <- lubridate::floor_date(out$date,"week")
         out$month <- lubridate::floor_date(out$date,"month")
         out$year <- lubridate::floor_date(out$date,"year")
         out$month.label <- paste(lubridate::year(out$date),lubridate::month(out$date,label = F),lubridate::month(out$date,label = T),sep="-")
         out$week.label <- paste(lubridate::year(out$date),lubridate::week(out$date),sep="-")
         out$year.label <- lubridate::year(out$date)
       }
       return(out)
     },
     aggregateData = function(data=NULL,groupby=NULL,addTotals=F){
       if(is.null(data)){
         d <- .self$data$latest
       }else{
         d <- data
       }
       .self$debugPrint(paste("Aggregating data by",groupby))
       if(!is.null(groupby)){
         groupby <- unlist(strsplit(groupby,","))
         dots <- lapply(groupby,as.symbol)
       }else{
         dots <- NULL
       }  
       agg <- d %>% dplyr::group_by_(.dots=dots) %>% dplyr::summarise(
         impressions = sum(impressions),
         clicks = sum(clicks),
         avg.position = mean(position,na.rm=T),
         ctr = round(clicks/impressions,2)
       )
       if(addTotals){
         .self$debugPrint(paste("Adding totals"))
         totalRow <- .self$aggregateData(d,groupby=NULL)
         totalRow[is.na(totalRow)]<-0
         totalRowHead <- data.frame(
           matrix(rep("-",length(groupby)),1, length(groupby),dimnames=list(c(), groupby)),
           stringsAsFactors=F
         )  
         totalRow <- cbind(totalRowHead,totalRow)
         agg <- rbind(as.data.frame(agg),totalRow)
       }
       .self$data$agg <- agg
       return(agg)
     },
     saveData = function(slot=NULL,filename=NULL){
       if(is.null(filename)){
         filename <- paste0(.self$paths$store,"search.rds")
       }
       if(is.null(slot)){
         d <- .self$data$latest
       }else{
         d <- .self$data[[slot]]
       }
       .self$debugPrint(paste("Data has",nrow(d),"rows"))
       .self$debugPrint(paste0("Saving data as ",filename))
       try(saveRDS(d,filename))
       invisible(return(TRUE))
     },
     initialize = function(auth,property,store="/data/ga/",debug=F){
       
       if(!require("searchConsoleR") || !require("googleAuthR") || !require("rga") || !require("dplyr")){
         stop('Please install the "searchConsoleR", "httr", "dplyr" and "jsonlite" packages before using this class')
       }
       #initialize with login values
       .self$data <- list()
       .self$debug <- debug
       .self$paths <- list()
       .self$paths$auth <- googleAuthR::gar_auth_service(auth)
       .self$paths$store <- store
       .self$rounding <- 3
       .self$property <- property
       #reset the handle
       urlHandle<-handle_find("https://www.googleapis.com/analytics/")
       handle_reset(urlHandle$url)

       return(.self)
       
     }
    )
)