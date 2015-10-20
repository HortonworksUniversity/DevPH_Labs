REGISTER datafu-1.2.0.jar;
REGISTER /usr/lib/pig/piggybank.jar;

DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

clicks = LOAD 'clicks.csv' USING PigStorage(',') AS (id:int, time:long, url:chararray);

clicks_iso = FOREACH clicks GENERATE UnixToISO(time) AS isotime, time, id;

clicks_group = GROUP clicks_iso BY id;

