package top.xiesen.flink.hadoop.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import top.xiesen.flink.common.model.LogType;

import static top.xiesen.flink.hadoop.constant.HadoopConstants.*;

/**
 * @Description hadoop sink
 * @className top.xiesen.flink.hadoop.sink.HadoopSink
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 14:59
 */
public class HadoopSink {
    /**
     * sink
     *
     * @param parameterTool ParameterTool
     * @return BucketingSink<ZorkLogData>
     */
    public static BucketingSink<LogType> getSink(ParameterTool parameterTool) {
        BucketingSink<LogType> sink = new BucketingSink<LogType>(parameterTool.get(HDFS_URL));
        sink.setBucketer(new CustomBucketer(parameterTool.get(PARTITION_FIELD_NAME, DEFAULT_PARTITION_FIELD_NAME)));
        sink.setWriter(new CustomWriter());
        sink.setBatchSize(parameterTool.getLong(HDFS_BATCH_SIZE, DEFAULT_HDFS_BATCH_SIZE)); // 默认是 128M
        sink.setBatchRolloverInterval(parameterTool.getLong(HDFS_BATCH_ROLLOVER_INTERVAL, DEFAULT_HDFS_BATCH_ROLLOVER_INTERVAL)); // 默认是10分钟
        sink.setPendingPrefix("");
        sink.setPendingSuffix("");
        sink.setInProgressPrefix(".");
        return sink;
    }
}
