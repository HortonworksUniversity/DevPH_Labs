create table names (id int, name string)
     partitioned by (state string)
     row format delimited fields terminated by '\t';

