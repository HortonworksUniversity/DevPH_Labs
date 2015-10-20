drop table if exists hvac;

-- Date,Time,TargetTemp,ActualTemp,System,SystemAge,BuildingID
create table hvac (readingdate string, readingtime string, targettemp int, actualtemp int, system int, systemage int, buildingid int) row format delimited fields terminated by ',' stored as TEXTFILE;

load data local inpath 'HVAC.csv' into table hvac; 

drop table if exists building;

-- BuildingID,BuildingMgr,BuildingAge,HVACproduct,Country
create table building (buildingid int, buildingmgr string, buildingage int, hvacproduct string, country string) row format delimited fields terminated by ',' stored as TEXTFILE;

load data local inpath 'building.csv' into table building;