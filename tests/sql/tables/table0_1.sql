CREATE TABLE database1.table0_1 ON CLUSTER cluster0
(
    `type0` String,
    `type1` UInt64,
    `type2` Float64
) 
ENGINE = Distributed('cluster0', 'database1', 'table0_2', cityHash64(`type0`))








