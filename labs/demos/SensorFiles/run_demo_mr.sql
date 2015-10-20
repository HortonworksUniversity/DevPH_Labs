set hive.execution.engine=mr;

select h.*, b.country, b.hvacproduct, b.buildingage, b.buildingmgr 
from building b join hvac h 
on b.buildingid = h.buildingid;

