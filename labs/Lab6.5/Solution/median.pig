REGISTER /usr/hdp/current/pig-client/lib/datafu.jar; 

define Median datafu.pig.stats.Median();

stocks = LOAD 'stocks.csv' USING PigStorage(',') AS (
   nyse:chararray, 
   symbol:chararray, 
   closingdate:chararray,
   openprice:double,
   highprice:double,
   lowprice:double);
   
stocks_filter = FILTER stocks BY highprice is not null;
stocks_group = GROUP stocks_filter BY symbol;

medians = FOREACH stocks_group {
	sorted = ORDER stocks_filter BY highprice;
	GENERATE group AS symbol, Median(sorted.highprice) AS median;
};

dump medians;
