#!/usr/bin/env bash
$FLINK_HOME/bin/flink run \
-c com.zorkdata.task.Log2EsMain \
${project.artifactId}-${project.version}.jar -d