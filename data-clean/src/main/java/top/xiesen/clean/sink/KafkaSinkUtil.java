package top.xiesen.clean.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

import static top.xiesen.flink.common.constant.PropertiesConstants.*;
import static top.xiesen.flink.common.constant.PropertiesConstants.DEFAULT_KAFKA_BROKERS;

/**
 * @Description
 * @className top.xiesen.clean.sink.KafkaSinkUtil
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 13:49
 */
public class KafkaSinkUtil {
    public static FlinkKafkaProducer011<String> sink(ParameterTool parameterTool) {
        String outTopic = parameterTool.get(KAFKA_SINK_TOPIC, DEFAULT_KAFKA_SINK_TOPIC);
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", parameterTool.get(KAFKA_SINK_BROKERS, DEFAULT_KAFKA_BROKERS));
        // 第一种方案, 设置 FlinkKafkaProducer011 里面事务超时时间
        // 设置事务超时时间
        outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "");

        // 第二种方案, 设置 kafka的最大事务超时时间
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(outTopic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                outProp,
                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
        return myProducer;
    }
}

