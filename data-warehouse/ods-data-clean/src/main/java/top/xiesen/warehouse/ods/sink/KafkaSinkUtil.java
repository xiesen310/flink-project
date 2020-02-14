package top.xiesen.warehouse.ods.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

import static top.xiesen.flink.common.constant.PropertiesConstants.*;

/**
 * @Description kafka sink
 * @className top.xiesen.warehouse.ods.sink.KafkaSinkUtil
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 11:29
 */
public class KafkaSinkUtil {

    /**
     * sink kafkaProducer
     *
     * @param parameterTool ParameterTool
     * @return FlinkKafkaProducer011<String>
     */
    public static FlinkKafkaProducer011<String> kafkaProducer(ParameterTool parameterTool) {
        String outputTopic = parameterTool.get(KAFKA_SINK_TOPIC, DEFAULT_KAFKA_SINK_TOPIC);
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", parameterTool.get(KAFKA_SINK_BROKERS, DEFAULT_KAFKA_BROKERS));
        outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "");
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(outputTopic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                outProp,
                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
        return myProducer;
    }

}
