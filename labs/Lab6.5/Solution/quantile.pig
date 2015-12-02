REGISTER /usr/hdp/current/pig-client/lib/datafu.jar; 

define Quantile datafu.pig.stats.Quantile('0.0','0.25','0.50','0.75','1.0');

stocks = LOAD 'stocks.csv' USING PigStorage(',') AS (
   nyse:chararray, 
   symbol:chararray, 
   closingdate:chararray,
   openprice:double,
   highprice:double,
   lowprice:double);

stocks_filter = FILTER stocks BY highprice is not null;
stocks_group = GROUP stocks_filter BY symbol;

quantiles = FOREACH stocks_group {
	sorted = ORDER stocks_filter BY highprice;
	GENERATE group AS symbol, Quantile(sorted.highprice) AS quant;
};

dump quantiles;
