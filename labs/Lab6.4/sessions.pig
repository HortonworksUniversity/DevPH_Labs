REGISTER /usr/hdp/current/pig-client/lib/datafu.jar; 
REGISTER /usr/hdp/current/pig-client/lib/piggybank.jar;

DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

clicks = LOAD 'clicks.csv' USING PigStorage(',') AS (id:int, time:long, url:chararray);

clicks_iso = FOREACH clicks GENERATE UnixToISO(time) AS isotime, time, id;

clicks_group = GROUP clicks_iso BY id;

