drop table if exists bucketnames;

create table bucketnames  (
     id int, name string, state string)
clustered by (id) into 3 buckets
row format delimited fields terminated by '\t';

