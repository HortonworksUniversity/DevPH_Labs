register /usr/hdp/current/pig-client/lib/datafu.jar; 

define PageRank datafu.pig.linkanalysis.PageRank();

topic_edges = LOAD '/user/root/edges.txt' as (topic:INT,source:INT,dest:INT,weight:DOUBLE);
topic_edges_grouped = GROUP topic_edges by (topic, source);

topic_edges_data = FOREACH topic_edges_grouped GENERATE group.topic as topic, group.source as source,topic_edges.(dest,weight) as edges;
topic_edges_data_by_topic = GROUP topic_edges_data BY topic;

topic_ranks = FOREACH topic_edges_data_by_topic GENERATE group as topic, FLATTEN(PageRank(topic_edges_data.(source,edges)));
store topic_ranks into 'topicranks';
