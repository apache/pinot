stlProcess <- function(time.series.data, seasonal) {
   ts.data <- ts(time.series.data, frequency = seasonal)
   stl.obj <- try(stl(ts.data, s.window="periodic", robust=TRUE, na.action=na.omit))

   if(class(stl.obj) == "try-error") warning('STL Error');
   stl.seasonal.factor <- stl.obj$time.series[,"seasonal"]
   stl.trend <- stl.obj$time.series[,"trend"]
   stl.residual <- stl.obj$time.series[,"remainder"]

   return(stl.trend+stl.residual)
}

seasonality <- 168

data <- read.csv(file='stdin', header=TRUE)
data$Bucket <- as.character(data$Bucket)
subdata <- data[,c('Bucket','Median')]
subdata <- subdata[order(data$Bucket),]

stlResult <- stlProcess(subdata$Median, seasonality)
write.table(stlResult, file='/dev/stdout', sep=',')
