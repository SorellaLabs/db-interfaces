CREATE TABLE database2.table1_1 ON CLUSTER cluster0
(
    `type0` String
) 
ENGINE = AggregatingMergeTree()
ORDER BY `type0`
