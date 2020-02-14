package top.xiesen.warehouse.ods.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import static top.xiesen.flink.common.constant.PropertiesConstants.*;

/**
 * 数据格式:
 * {
 * "@timestamp": "2020-02-14T02:13:03.603Z",
 * "@metadata": {
 * "beat": "filebeat",
 * "type": "doc",
 * "version": "6.8.6",
 * "topic": "gmall-log"
 * },
 * "message": "1581646382708|{\"cm\":{\"ln\":\"-53.9\",\"sv\":\"V2.2.9\",\"os\":\"8.2.5\",\"g\":\"S166LT26@gmail.com\",\"mid\":\"996\",\"nw\":\"3G\",\"l\":\"en\",\"vc\":\"7\",\"hw\":\"1080*1920\",\"ar\":\"MX\",\"uid\":\"996\",\"t\":\"1581552575618\",\"la\":\"23.4\",\"md\":\"sumsung-18\",\"vn\":\"1.2.3\",\"ba\":\"Sumsung\",\"sr\":\"G\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1581585861898\",\"en\":\"start\",\"kv\":{\"entry\":\"4\",\"loading_time\":\"17\",\"action\":\"1\",\"open_ad_type\":\"2\",\"detail\":\"\"}},{\"ett\":\"1581631167953\",\"en\":\"ad\",\"kv\":{\"entry\":\"2\",\"show_style\":\"3\",\"action\":\"3\",\"detail\":\"\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"7\"}},{\"ett\":\"1581589396288\",\"en\":\"favorites\",\"kv\":{\"course_id\":7,\"id\":0,\"add_time\":\"1581612360893\",\"userid\":4}}]}",
 * "source": "/opt/module/logs/app-2020-02-14.log",
 * "input": {
 * "type": "log"
 * },
 * "fields": {
 * "app": "taobao"
 * },
 * "beat": {
 * "name": "s104",
 * "hostname": "s104",
 * "version": "6.8.6"
 * },
 * "host": {
 * "name": "s104"
 * },
 * "offset": 5933304,
 * "log": {
 * "file": {
 * "path": "/opt/module/logs/app-2020-02-14.log"
 * }
 * },
 * "prospector": {
 * "type": "log"
 * }
 * }
 *
 * @Description kafka 数据源二次开发
 * @className top.xiesen.warehouse.ods.source.KafkaSourceUtil
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 10:26
 */
public class KafkaSourceUtil {

    /**
     * 添加 kafka 数据源
     *
     * @param env           StreamExecutionEnvironment
     * @param parameterTool ParameterTool
     * @return DataStream<String>
     */
    public static DataStream<String> addSource(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        String topic = parameterTool.get(LOGS_TOPIC);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", parameterTool.get(KAFKA_BROKERS, DEFAULT_KAFKA_BROKERS));
        prop.setProperty("group.id", parameterTool.get(KAFKA_GROUP_ID, DEFAULT_KAFKA_GROUP_ID));

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);

        DataStream<String> data = env.addSource(kafkaConsumer).uid("kafka-source");
        return data;
    }
}
