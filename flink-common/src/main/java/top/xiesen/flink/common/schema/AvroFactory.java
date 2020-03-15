package top.xiesen.flink.common.schema;

/**
 * @Description AvroFactory
 * @className top.xiesen.flink.common.schema.AvroFactory
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:58
 */
public class AvroFactory {
    private static AvroSerializer metricMetadata = null;
    private static AvroSerializer logMetadata = null;

    private static AvroDeserializer logs = null;
    private static AvroDeserializer metrics = null;

    public static void init() {
        logs = null;
        metrics = null;
    }

    /**
     * getLogsDeserializer
     *
     * @return AvroDeserializer
     */
    public static AvroDeserializer getLogsDeserializer() {
        if (logs == null) {
            logs = new AvroDeserializer(AvroSchemaDef.ZORK_LOG_SCHEMA);
        }
        return logs;
    }

    /**
     * getMetricDeserializer
     *
     * @return AvroDeserializer
     */
    public static AvroDeserializer getMetricDeserializer() {
        if (metrics == null) {
            metrics = new AvroDeserializer(AvroSchemaDef.ZORK_METRIC_SCHEMA);
        }
        return metrics;
    }

    /**
     * getLogAvroSerializer
     *
     * @return AvroSerializer
     */
    public static AvroSerializer getLogAvroSerializer() {
        if (logMetadata == null) {
            logMetadata = new AvroSerializer(AvroSchemaDef.ZORK_LOG_SCHEMA);
        }
        return logMetadata;
    }


    /**
     * getMetricAvroSerializer
     *
     * @return AvroSerializer
     */
    public static AvroSerializer getMetricAvroSerializer() {
        if (metricMetadata == null) {
            metricMetadata = new AvroSerializer(AvroSchemaDef.ZORK_METRIC_SCHEMA);
        }
        return metricMetadata;
    }

}
