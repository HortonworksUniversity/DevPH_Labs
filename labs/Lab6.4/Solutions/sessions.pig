REGISTER /usr/hdp/current/pig-client/lib/datafu.jar; 
REGISTER /usr/hdp/current/pig-client/lib/piggybank.jar;

DEFINE Sessionize datafu.pig.sessions.Sessionize('8m');
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE Median datafu.pig.stats.Median();

clicks = LOAD 'clicks.csv' USING PigStorage(',') AS (id:int, time:long, url:chararray);

clicks_iso = FOREACH clicks GENERATE UnixToISO(time) AS isotime, time, id;

clicks_group = GROUP clicks_iso BY id;

clicks_sessionized = FOREACH clicks_group {
	sorted = ORDER clicks_iso BY isotime;
	GENERATE FLATTEN(Sessionize(sorted)) AS (isotime, time, id, sessionid);
};

--dump clicks_sessionized;

sessions = FOREACH clicks_sessionized GENERATE time, sessionid;
sessions_group = GROUP sessions BY sessionid;

session_times = FOREACH sessions_group GENERATE group as sessionid, (MAX(sessions.time) - MIN(sessions.time)) / 1000.0 / 60 as session_length;

--dump session_times;
sessiontimes_all = GROUP session_times ALL;
sessiontimes_avg = FOREACH sessiontimes_all {
	ordered = ORDER session_times BY session_length;
	GENERATE AVG(ordered.session_length) AS avg_session,
	   Median(ordered.session_length) AS median_session;
}; 

dump sessiontimes_avg;

