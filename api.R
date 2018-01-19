
googleAnalytics <- setRefClass("googleAnalytics",
    fields = list(
      debug = "logical",
      clientId = "character",
      clientSecret = "character",
      metrics = "character",
      dimensions = "character",
      paths = "list",
      from = "character",
      to = "character",
      splits = "character",
      properties = "list",
      initializeTime = "POSIXt",
      rounding ="numeric",
      spam = "list",
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
      getProperties = function(){
        return(.self$properties)
      },
      getData = function(lazyLoad=F,dimensions=NULL,metrics=NULL,from=NULL,to=NULL){
        if(lazyLoad){
          out <- readRDS(paste0(.self$paths$store,"data.rds"))
          return(out)
        }
        if(!is.null(dimensions)){
          .self$dimensions <- dimensions
        }
        if(!is.null(metrics)){
          .self$metrics <- metrics
        }
        if(!is.null(from)){
          .self$from <- from
        }
        if(!is.null(to)){
          .self$to <- to
        }
        out <- list()
        splitsFrom <- seq(as.Date(.self$from), as.Date(.self$to), by = .self$splits) 
        splitsTo <- c(splitsFrom[-1],as.Date(.self$to))
        
        for(property in .self$properties){
          l <- mapply(function(p,f,t,s){
            d<-try(ga$getData(
              p, f, t, batch = TRUE,
              metrics = .self$metrics,
              dimensions = .self$dimensions,
              segment=s,
              start = 1
            ))
            if(inherits(d,"try-error")){
              d<-NULL
            }else{
              d<-as.data.frame(d)
            }
            return(d)
          },p=property['id'],s=property['segment'],f=splitsFrom,t=splitsTo,SIMPLIFY=F)
          l <- do.call("rbind",l)
          l$property <- property['name']
          out[[property['name']]]<-l
        }
        out <- do.call("rbind",out)
        cols <- colnames(out)
        
        if('date' %in% cols){
          out$week <- lubridate::floor_date(out$date,"week")
          out$month <- lubridate::floor_date(out$date,"month")
          out$year <- lubridate::floor_date(out$date,"year")
          out$month.label <- paste(lubridate::year(out$date),lubridate::month(out$date,label = F),lubridate::month(out$date,label = T),sep="-")
          out$week.label <- paste(lubridate::year(out$date),lubridate::week(out$date),sep="-")
          out$year.label <- lubridate::year(out$date)
        }
        if('sessions' %in% cols & 'bounces' %in% cols){
          out$stays <- out$sessions - out$bounces
        }
        if('landingPagePath' %in% cols){
          out$language <- .self$getLanguage(out$landingPagePath)
          out$landingPage <- sapply(strsplit(out$landingPagePath,"?",fixed=T),function(x) x[1])
          out$query <- sapply(strsplit(out$landingPagePath,"?",fixed=T),function(x) x[2])
          out$icid <- unlist(urltools::param_get(tolower(out$landingPagePath),"icid"))
          out$uid <- unlist(urltools::param_get(out$landingPagePath,"uid"))
          out$path <- gsub(pattern = "/mapp.com" ,replacement = "", x=out$landingPage)
        }
        if('source' %in% cols){
          out <- out[!grepl(.self$spam$source,out$source),]
        }
        if('fullreferrer' %in% cols){
          out <- out[!grepl(.self$spam$fullreferrer,out$fullreferrer),]
        }
        if(all(c('source','medium','query') %in% cols)){
          out$origin <- apply(data$base,1,.self$getTrafficSource)
        }
        .self$data$latest <- out
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
          sessions = sum(sessions),
          bounces = sum(bounces),
          stays = sum(sessions-bounces),
          bounce.rate = round(bounces/sessions,2),
          organic.share = round(sum(sessions[origin=="organic"])/sessions,2)
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
          filename <- paste0(.self$paths$store,"data.rds")
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
      getLanguage = function(input){
        
        sapply(input,function(i){
          if(grepl("/de/",i,fixed=T)){
            return("de")
          } else if(grepl("/fr/",i,fixed=T)){
            return("fr")
          } else if(grepl("/it/",i,fixed=T)){
            return("it")
          } else if(grepl("/es/",i,fixed=T)){
            return("it")
          } else{
            return("en")
          }
        })
        
      },
      getTrafficDestination = function(input,fromColumn="landingPagePath"){
        patterns <- list(
          home = "mapp\\.com",
          cep = "customer-engagement-platform|email-marketing",
          dmp = "data-management|dmp|datenverwaltung|let-your-data",
          services = "services|dienstleistungen|servizi",
          blog = "blog",
          about = "leadership|mentions|politique|carierre|legali|about|story|locations|apropos|ueber|acceptable|datenschutz|legal|privacy|informazioni|impressum",
          resources = "resources|ressourcen|centre-de-ressources",
          careers = "careers|karriere",
          demo = "demo"
        )
        stopifnot(fromColumn %in% names(input))
        i <- tolower(c(input[fromColumn]))
        matches <- try(sapply(patterns,grepl,i))
        
        if(inherits(matches,"try-error")){
          return("home")
        }else{
          weightedMatches <- matches * (1+1:length(patterns)/10)
          bestMatch <- which.max(weightedMatches)
          return(names(patterns)[bestMatch])
        }
        

      },
      getTrafficSource = function(input){
          patterns <- list(
              referral = c(
                medium = "referral"
              ),
              direct = c(
                source = "(direct)"
              ),
              organic = c(
                source="yandex|wikipedia|bing|yahoo|duckduck|yahoo|google$",
                medium="search|organic"
              ),
              social = c(
                source="t\\.co|plus\\.google|Sigstr|baidu|whatsapp|xing|facebook|linkedin|twitter|social|youtube|reddit|instagram|pinterest|slideshare|lnkd\\.in",
                medium="social",
                query="cepsocial|cep_social"
              ),
              ads_social = c(
                medium="ads_social",
                query="icid\\="
              ),
              email = c(
                medium="email",
                query="mid\\=|icid\\=email"
              ),
              sea = c(
                source="cpc|cpm|gsn|gdn",
                medium="cpc|cpm|gsn|gdn"
              ),
              ads_pr = c(
                source="outbrain|mycustomer|emarketing|e-marketing\\.fr|emailmarketingblog|koschadepr|engage\\.it|programmatic-italia|prnewswire",
                medium="banner|display|mediabuy|content|free_company_page"
              ),
              own_domains= c(
                source="CEP_footer|mapp|teradata|marketing\\.teradata|applications\\.teradata|blogs\\.teradata|help\\.teradatadmc|cust-mta|localhost|aprimo|ec-messenger|ecircle|sslg\\.teradatadmc|sslh\\.teradatadmc|wiki.\\mapp|td-campaigns|domeus|marlinequity|flxone|bluehornet|argyle|appoxee|applications\\.teradata",
                medium="product"
              ),
              blogs = c(
                source="blog",
                medium="blog"
              ),
              customer_domains = c(
                source="dolcegabbana|austrade|dunnesstores|email-telekom\\.de|email\\.pepsico|thomascook|centerparcs|obi\\.|mailing\\.|news\\.|newsletter\\.|westwing|adac\\.de|bonprix|virginmoney|lascana|hilti|douglas|springer|blacks\\.co|engbers|karstadt|swarowski|o2online|sportscheck|otto|modomoto|weltbild|victoriabeckham|mag-mobile|read_message\\.jsp|magmobile"
              )
          )
          i <- tolower(c(input['medium'],input['source'],input['query']))
          matches <- try(sapply(patterns,function(pat){
            i <- i[names(i) %in% names(pat)]
            s <- sapply(i[names(pat)],grepl,pat)
            o <- sum(diag(sapply(i[names(pat)], function(j) grepl(pattern=pat,x=j))),na.rm=T)
            return(o)
          }))
          
          if(inherits(matches,"try-error")){
            return("direct")
          }else{
            weightedMatches <- matches * (1+1:length(patterns)/5)
            bestMatch <- which.max(weightedMatches)
          }

          #enforcement
          if(input['medium']=="ads_social"){
            return("ads_social")
          }
          
          return(names(patterns)[bestMatch])
      },
      settingsExample = function(){
        settingsExample <-''
        return(jsonlite::prettify(settingsExample))
      },
      settingsCheck = function(settings){
        return(settings)
      },
      initialize = function(settings=settings,metrics,dimensions,from="2014-09-01",to=as.character(Sys.Date()),debug=F,store="/data/ga/"){
        
        if(missing(settings)){
          stop("Please supply the settings as JSON, list or environment object")
        }else{
          settings <- .self$settingsCheck(settings)
        }  
        if(!require("httr") || !require("jsonlite") || !require("rga") || !require("dplyr")){
          stop('Please install the "rga", "httr", "dplyr" and "jsonlite" packages before using this class')
        }
        #initialize with login values
        .self$data <- list()
        .self$debug <- debug
        .self$clientId <- settings$ga$clientId
        .self$clientSecret <- settings$ga$clientSecret
        .self$paths <- list()
        .self$paths$auth <- settings$ga$path
        .self$paths$store <- store
        .self$from <- from
        .self$to <- to
        .self$splits <- settings$ga$splits
        .self$rounding <- 3
        .self$spam <- list(source="motherboard\\.vice|lifehac|google-liar",fullreferrer="motherboard\\.vice|lifehac|google-liar")
        if(missing(metrics)){
          .self$metrics <- settings$ga$metrics
        }else{
          .self$metrics <- metrics
        }
        if(missing(dimensions)){
          .self$dimensions<- settings$ga$dimensions
        }else{
          .self$dimensions <- dimensions
        }
        .self$properties <- list(
          c(name="marketing.teradata.com",id="ga:89265300",segment="gaid::-1"),
          c(name="news.mapp.com",id="ga:115648520",segment="gaid::-1"),
          c(name="mapp.com",id="ga:129673294",segment="gaid::0jHYYltTTRSprEUWuL5pfA"),
          c(name="blogs.teradata.com",id="ga:34963074",segment="gaid::e1HvL8F4Sh6PHWoyFZwGyw")
        )
        #reset the handle
        urlHandle<-handle_find("https://www.googleapis.com/analytics/")
        handle_reset(urlHandle$url)

        if(!file.exists(.self$paths$auth)){
          rga.open(instance = "ga",client.id = .self$clientId,client.secret = .self$clientSecret)
          saveRDS(ga,file=.self$paths$auth)
          ga<-readRDS(.self$paths$auth)
          assign("ga",ga,envir=.GlobalEnv)
        }else{
          ga<-readRDS(.self$paths$auth)
          assign("ga",ga,envir=.GlobalEnv)
        }
        
        return(.self)
        
      }
    )
)

