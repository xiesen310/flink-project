#!/bin/bash
$FLINK_HOME/bin/flink run \
-c com.zorkdata.hdfs.Log2DFS \
log2hdfs-1.0.0.jar -d