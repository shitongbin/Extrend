library(data.table)
library(dplyr)
library(slider)
library(tidyr)

options(scipen =200)


#读取数据
daily_detail <- fread('/root/superRev/deploy/modelA/detailDatas.csv', encoding = 'UTF-8')
daily_basic <- fread('/root/superRev/deploy/modelA/baiscDatas.csv', encoding = 'UTF-8')
base_stock <- fread('/root/superRev/deploy/modelA/stock.csv', encoding = 'UTF-8')
trade_cal <- fread('/root/superRev/deploy/modelA/calList.csv', encoding = 'UTF-8')

#

now_date <- as.numeric(gsub("-", "", as.character(Sys.Date()))) 


if(now_date %in% trade_cal$cal_date){
  df <- merge(daily_detail, daily_basic[,-3, with = F], by = c("ts_code","trade_date"), all = F)[,.(ts_code, trade_date,open, high, low, close,pct_chg,amount,ma50,ma120,ma200,turnover_rate_f,pe,pb,pe_ttm, total_mv)]
  df <- arrange(df, ts_code, trade_date)
  
  #筛选样本，剔除样例不足样本
  deLnames <- names(which(table(df$ts_code) < 50))
  df <- df[!df$ts_code %in% deLnames]
  
  
  df[,`:=`(rank = slide_dbl(close,function(x) rank(-x,ties.method = 'first')[length(x)] ,.before = 90),
           rank_diff = slide_dbl(close, function(x) {
             rank_seris = rank(-x,ties.method = 'first')
             firsr_index = which(rank_seris == 1) 
             sec_index = which(rank_seris == 2) 
             ture_sec_index = ifelse(length(sec_index)> 0 ,sec_index, 1)
             return(firsr_index - ture_sec_index)} ,.before = 90)
  ), by = ts_code]
  

  attach(df)
  df$buy_flag <- ifelse(
    rank == 1 &
      rank_diff > 8 &
      rank_diff < 30 &
      turnover_rate_f < 4.5 &
      pct_chg > 3 &
      pct_chg < 7 &
      close > ma50 &
      amount > 10000 &
      (pe_ttm < 150 | is.na(pe_ttm)) &
      pb < 20 ,
    1,
    0
  )
  
  detach(df)
  df <- merge(df, base_stock[,-2,with = F], by = "ts_code", all.x = T)
  
  print(df[buy_flag == 1 & trade_date == now_date])
} 


df_tmp <- df[df$trade_date == now_date ,c("ts_code","close")]
colnames(df_tmp)[2] <- 'now_price'
result2 <- df[buy_flag == 1 & trade_date > '20211001', c("ts_code","trade_date","close","name","industry")][order(-trade_date)]
com1 <- merge(result2, df_tmp, by = c("ts_code"),all.x = T)[order(-trade_date)]
result <- com1[,.(trade_date, name, industry)]
write.csv(head(result,20),file = '/root/Extrend.csv',row.names = F, quote = F)