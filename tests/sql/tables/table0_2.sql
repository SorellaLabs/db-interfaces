CREATE TABLE database1.table0_2 ON CLUSTER cluster0
(
    `type0` String,
    `type1` UInt64,
    `type2` Float64
) 
ENGINE = ReplicatedReplacingMergeTree('/path/to/zookeeper/', '{replica}')
ORDER BY (`type0`)



