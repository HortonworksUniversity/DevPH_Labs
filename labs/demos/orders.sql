create table orders  (
cid int,
price double,
quantity int
)
ROW FORMAT DELIMITED 

FIELDS TERMINATED BY '\t';

load data local inpath '/root/labs/demos/orders.txt' overwrite into table orders;