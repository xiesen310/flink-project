package top.xiesen.flink.hadoop.sink;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.hadoop.fs.Path;
import top.xiesen.flink.common.model.LogType;
import top.xiesen.flink.hadoop.utils.ReflectionUtil;

/**
 * @Description
 * @className top.xiesen.flink.hadoop.sink.CustomBucketer
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 15:02
 */
public class CustomBucketer extends BasePathBucketer<LogType> {
    private String partitionField;

    public CustomBucketer(String partitionField) {
        this.partitionField = partitionField;
    }

    @Override
    public Path getBucketPath(Clock clock, Path basePath, LogType element) {
        String path = ReflectionUtil.getPartition(element, partitionField);
        return new Path(basePath + path);
    }
}
