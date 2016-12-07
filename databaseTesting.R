library(data.table)
library(RODBC)
channel <- odbcConnectAccess2007("C:/Users/lsiv67/Documents/Database1.accdb") ##connects to the DB

##For testing only
sqlQuery(channel,"Delete * from filenames")
sqlQuery(channel,"Delete * from connectedtable")
sqlQuery(channel,"Delete * from compoundlist")
sqlQuery(channel,"Delete * from istdconc")

tidyData <- function(rawDat){
  rawDat[1,] <- lapply(rawDat[1,],function(y) gsub(" Results","",y))
  if(rawDat[2,2]=="" & !is.na(rawDat[2,2])){
    rawDat[2,2] <- "a"
    count = 6
  } else {
    count = 5
  }
  
  # Fill in compound name in empty columns (different parameters of the same compound)
  for(c in 1:ncol(rawDat)){
    val = rawDat[1,c]
    if((!is.na(val)) && nchar(val,keepNA = TRUE) > 0 ){
      colname=val
    } else {
      rawDat[1,c]=colname
    }
  }
  
  # Concatenate rows containing parameters + compounds to the form parameter.compound for conversion with reshape() 
  rawDat[1,] <- paste(rawDat[2,], rawDat[1,],sep = ".")
  colnames(rawDat) <- rawDat[1, ]
  rawDat = rawDat[-1:-2,]
  
  # Replace column header names and remove columns that are not needed or not informative 
  setnames(rawDat, old=c(".Sample","Data File.Sample", "Name.Sample", "Acq. Date-Time.Sample", "Type.Sample"), new=c("QuantWarning","SampleFileName","SampleName", "AcqTime", "SampleTypeMethod"))
  rawDat = rawDat[, !names(rawDat) %in% c("NA.Sample","Level.Sample")]
  
  # Transform wide to long (tidy) table format
  datLong=reshape(rawDat,idvar = "SampleFileName", varying = colnames(rawDat[,-1:-count]), direction = "long",sep = "." )
  row.names(datLong) <- NULL
  setnames(datLong, old=c("time"), new=c("Compound"))
  
  # Covert to data.table object and change column types
  dat <- dplyr::tbl_df(datLong)
  rawDat <- dplyr::tbl_df(rawDat)
  numCol <- c("RT","Area","Height", "FWHM")
  dat[, names(dat) %in% numCol] <- lapply(dat[,names(dat) %in% numCol], as.numeric)
  factCol <- c("QuantWarning","SampleName","SampleFileName","SampleTypeMethod")
  dat[, names(dat) %in% factCol] <- lapply(dat[,names(dat) %in% factCol], as.factor)
  dat$Compound <- trimws(dat$Compound)
  
  return(dat)
}




datWide <- read.csv("S:/Sample data/TK02_LPAF-LPC-MHC-Cer_Data_V2.csv", header=FALSE, sep=",", na.strings=c("#N/A", "NULL"), check.names=FALSE, as.is=TRUE, strip.white=TRUE, stringsAsFactors = FALSE)
mapISTD <- read.csv("S:/Sample data/TK02_LPAF-LPC-MHC-Cer_ISTDmap.csv", header = TRUE, sep = ",", check.names=TRUE, as.is=TRUE, strip.white = TRUE, stringsAsFactors = FALSE)  
ISTDDetails <- read.csv("S:/Sample data/TK02_LPAF-LPC-MHC-Cer_ISTDconc.csv", stringsAsFactors = FALSE)

##This line writes data to the filenames table
b <- datWide[3:nrow(datWide),3:7]
apply(b,1,function(a){sqlQuery(channel,paste("INSERT INTO filenames VALUES (\'",a[2],"\',\'",a[5],"\',\'",a[3],"\',\'",a[1],"\')",sep = ""))})

##This line writes data to the compoundlist table
apply(mapISTD,1,function(a){sqlQuery(channel,paste("INSERT INTO compoundlist VALUES (\'",a[1], "\',\'", a[2], "\')",sep=""))})

##This line writes data to the istdconc table
apply(ISTDDetails,1,function(a){sqlQuery(channel,paste("INSERT INTO istdconc VALUES (\'",a[1],"\',\'",a[2],"\',\'",a[3],"\')",sep=""))})

##----------------------------------------------------

##THE FOLLOWING CODE CHUNK WRITES DATA INTO THE CONNECTEDTABLE
##it is assumed that y is the long dataset with only the filename, compound, area, fwhm, and rt columns   

y <- tidyData(datWide)[c(3,6,7,8,9)]


i <<- 0

counter <- function(){
  i <<- i+1
  i
}

apply(y,1, function(a){sqlQuery(channel,paste("INSERT INTO connectedtable VALUES (\'", counter(), "\',\'", as.character(a[1]), "\',\'", a[2], "\',\'", a[3], "\',\'", a[4], "\',\'", a[5], "\')",sep=""))})

##----------------------------------------------------

odbcClose(channel)
