package top.xiesen.flink.common.constant;

/**
 * @Description
 * @className top.xiesen.flink.common.constant.PropertiesConstants
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/13 16:33
 */
public class PropertiesConstants {
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "flink-project";
    public static final String LOGS_TOPIC = "logs.topic";
    public static final String LATE_LOG_TOPIC = "late.log.topic";
    public static final String DEFAULT_LATE_LOG_TOPIC = "lateLog";

    /**
     * kafka sink
     */
    public static final String KAFKA_SINK_BROKERS = "kafka.sink.brokers";
    public static final String KAFKA_SINK_TOPIC = "kafka.sink.topic";
    public static final String DEFAULT_KAFKA_SINK_TOPIC = "kafka-default-sink";

    /**
     * redis
     */

    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";


    public static final String METRICS_TOPIC = "metrics.topic";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    public static final String CHECKPOINT_MEMORY = "memory";
    public static final String CHECKPOINT_FS = "fs";
    public static final String CHECKPOINT_ROCKETSDB = "rocksdb";

    /**
     * elasticsearch 相关
     */
    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    public static final String ELASTICSEARCH_URLS = "elasticsearch.urls";

    /**
     * mysql 相关
     */
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";
}
