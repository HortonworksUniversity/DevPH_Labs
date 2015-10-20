--whitehouse.pig


visits = LOAD '/user/root/whitehouse/visits.txt' USING PigStorage(',');

congress = FILTER visits BY ($25 MATCHES '.* CONGRESS .*');

projection_congress = FOREACH congress GENERATE
   $0 AS lname:chararray,
   $1 AS fname:chararray, 
   $2 AS middle:chararray, 
   $6 AS time_of_arrival:chararray, 
   $11 AS appt_scheduled_time:chararray, 
   $19 AS visitee_lname:chararray, 
   $20 AS visitee_fname:chararray, 
   $21 AS location:chararray, 
   $25 AS comment:chararray ;


STORE projection_congress INTO '/user/root/project_congress';

