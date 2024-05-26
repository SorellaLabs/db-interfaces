CREATE TABLE database1.`sub_db0.table0_3` ON CLUSTER cluster0
(
    `type0` String,
    `type1` UInt64,
    `type2` Float64
) 
ENGINE = ReplicatedReplacingMergeTree('/path/to/zookeeper/', '{replica}')
ORDER BY (`type0`)
