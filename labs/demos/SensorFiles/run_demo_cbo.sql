set hive.execution.engine=tez;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.partition.stats=false;
set hive.cbo.enable=true;
set hive.stats.fetch.column.stats=true;

explain select buildingid, max(targettemp-actualtemp)
from hvac group by buildingid;

select buildingid, max(targettemp-actualtemp)
from hvac group by buildingid;

