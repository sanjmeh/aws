pacman::p_load(janitor,glue,lubridate,data.table,jsonlite,magrittr,tidyverse,htmlTable,googlesheets4)

gen_csv <- function(ipath,tryfiles = NULL,isaddress=F){
  if(is.null(tryfiles))
    files <- list.files(ipath,full.names = T) else{
      if(is.na(as.numeric(tryfiles))) stop("pass an integer number of files in arg tryfiles")
      files <- files %>% head(tryfiles) 
    }
  if(length(files) > 0){
    x1 <- files %>% map(read_lines)
    files <- files[-c(which(lengths(x1)==0))]
    x1 <-  x1[-c(which(lengths(x1)==0))]
    x1 <- x1[-length(x1)]
    files <- files[-length(files)]
    x1a <- x1 %>%  map(fromJSON) %>% rbindlist() 
    x1a[,mtime:=files %>% file.info() %>% extract2("mtime")]
    # x1a <- x1[-length(x1)] %>%  map(~ifelse(is.na(.x) | is.null(.x) | length(.x)==0, data.table(),
    #                            tryCatch({fromJSON(.x) %>% as.data.table()},error = function(x) data.table()))) %>% 
    #   rbindlist()
    x1a[,mtime:=files %>% file.info() %>% extract2("mtime")]
    if(isaddress==T) x1a[,address:=ifelse(nchar(address)>20,address,NA)] else
      x1a[,address:=NA_character_]
    #x1a[,score:=context %>% str_extract("\\d$")]
    x1a
  } else
    stop("Zero files in the path.. make sure the path is a directory and not a file path")
}


