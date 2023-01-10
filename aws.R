# aws credentials for IAM access for programatic access using user: smRAPI
pacman::p_load(aws.s3,glue,data.table,janitor,aws.iam,aws.signature,lubridate,purrr,gt,tidyverse)
source("awscredentials.R")

blist <- bucketlist(key = Sys.getenv("AWS_ACCESS_KEY_ID") ,secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"))

# fetch bucket details - this may take around 5 to 10 seconds
fetch_bucket_keys <- function(nmax = 5000){
    buckets <- get_bucket(bucket = "bucket-ocean",
                          key = Sys.getenv("AWS_ACCESS_KEY_ID"),
                          secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                          max = nmax)
    rbindlist(buckets)
}

fetch_bucket_contents <- function(bucketdt,date = "2023-01-09",nmax = NULL){
  if(!is.null(nmax))
    keys <-bucketdt[as_date(LastModified) == "2023-01-09",Key] %>% head(nmax)
  else keys <-bucketdt[as_date(LastModified) == "2023-01-09",Key]
    x1 <-
    keys %>% 
        map(
            ~ try(s3read_using(FUN = data.table::fread, object = glue("s3://{bucketdt$Bucket[1]}/",.x),
                               opts = list(key = Sys.getenv("AWS_ACCESS_KEY_ID"),
                                           secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"))))
        )
    xdt <- rbindlist(x1) |> unique()
    xdt[,strt:=as.POSIXct(start,origin = "1970-01-01")]
    xdt[,stop:=as.POSIXct(end,origin = "1970-01-01")]
    xdt[,nbytes := as.integer(bytes)]
}

# overall summary counts
rep1 <- function(xdt){
    xdt[,.(.N,uniq_src_addr = uniqueN(srcaddr),avgbytes = mean(nbytes,na.rm=T)),
        .(date = as_date(strt))][,tx_per_addr := N/uniq_src_addr] |>
        gt() |>
        tab_header("bucket-ocean ana;ysis",
                   subtitle = glue("Total processed files:{nrow(dt1)}; total transations: {nrow(xdt)}")) |>
        fmt_integer(columns = c(avgbytes,tx_per_addr))
}

# top 20 heaviest source address
rep2 <- function(xdt){
    xdt[,.(.N,avgbytes = mean(nbytes,na.rm=T)),
        .(srcaddr)][order(-N)][1:20] |>
        gt() |>
        tab_header("Exceptionally high volume users",
                   subtitle = "top 20 source addresses") |>
        fmt_integer(columns = c(N,avgbytes))
}


