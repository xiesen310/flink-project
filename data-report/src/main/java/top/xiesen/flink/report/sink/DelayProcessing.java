package top.xiesen.flink.report.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import top.xiesen.flink.common.constant.PropertiesConstants;

import java.util.Properties;

import static top.xiesen.flink.common.constant.PropertiesConstants.KAFKA_BROKERS;

/**
 * @Description 延迟数据处理
 * @className top.xiesen.flink.report.sink.DelayProcessing
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/13 20:19
 */
public class DelayProcessing {
    public static void process(ParameterTool parameterTool, DataStream<Tuple3<Long, String, String>> sideOutput) {
        String outTopic = parameterTool.get(PropertiesConstants.LATE_LOG_TOPIC, PropertiesConstants.DEFAULT_LATE_LOG_TOPIC);
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", parameterTool.get(KAFKA_BROKERS
                , PropertiesConstants.DEFAULT_KAFKA_BROKERS));
        // 第一种方案, 设置 FlinkKafkaProducer011 里面事务超时时间
        // 设置事务超时时间
        outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "");

        // 第二种方案, 设置 kafka的最大事务超时时间
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(outTopic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                outProp,
                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        sideOutput.map(new MapFunction<Tuple3<Long, String, String>, String>() {
            @Override
            public String map(Tuple3<Long, String, String> value) throws Exception {
                return value.f0 + "\t" + value.f1 + "\t" + value.f2;
            }
        }).addSink(myProducer);
    }

}
