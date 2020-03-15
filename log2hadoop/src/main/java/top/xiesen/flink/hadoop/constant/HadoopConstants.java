package top.xiesen.flink.hadoop.constant;

/**
 * @Description
 * @className top.xiesen.flink.hadoop.constant.HadoopConstants
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 14:58
 */
public class HadoopConstants {

    /**
     * hadoop 相关配置
     */
    public static final String PARTITION_FIELD_NAME = "partition.field.name";
    public static final String DEFAULT_PARTITION_FIELD_NAME = "dimensions['appsystem'],logTypeName,timestamp";
    public static final String HDFS_BATCH_SIZE = "hdfs.batch.size";
    public static final Long DEFAULT_HDFS_BATCH_SIZE = 1024 * 1024 * 128L;
    public static final String HDFS_BATCH_ROLLOVER_INTERVAL = "hdfs.batch.rollover.interval";
    public static final Long DEFAULT_HDFS_BATCH_ROLLOVER_INTERVAL = 10 * 60 * 1000L;
    public static final String HDFS_URL = "hdfs.url";
}
