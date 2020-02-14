package top.xiesen.clean.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import static top.xiesen.flink.common.constant.PropertiesConstants.*;

/**
 * @Description kafka source
 * @className top.xiesen.clean.source.MyKafkaSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 13:35
 */
public class MyKafkaSource {

    public static FlinkKafkaConsumer011<String> source(ParameterTool parameterTool) {
        // 指定 kafka source
        String topic = parameterTool.get(LOGS_TOPIC);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", parameterTool.get(KAFKA_BROKERS, DEFAULT_KAFKA_BROKERS));
        prop.setProperty("group.id", parameterTool.get(KAFKA_GROUP_ID, DEFAULT_KAFKA_GROUP_ID));

        // 获取 kafka 中的数据
        // {"dt":"2020-02-12 11:11:11","countryCode":"US","data":[{"type:"s1","score"：“0.3”，“level”:"A"},{"type:"s1","score"：“0.3”，“level”:"A"}]}
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), prop);

        return kafkaSource;
    }


}
