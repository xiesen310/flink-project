package top.xiesen.warehouse.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import top.xiesen.flink.common.utils.CheckPointUtil;
import top.xiesen.flink.common.utils.ExecutionEnvUtil;

import java.util.Properties;

import static top.xiesen.flink.common.constant.PropertiesConstants.*;

/**
 * @Description 明细数据层(data warehouse detail)
 * @className top.xiesen.warehouse.dwd.DWDDataClean
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 12:55
 */
public class DwdDataClean {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env = CheckPointUtil.setCheckpointConfig(env, parameterTool);

        // 配置 kafka 数据源
        String topic = parameterTool.get(LOGS_TOPIC);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", parameterTool.get(KAFKA_BROKERS, DEFAULT_KAFKA_BROKERS));
        prop.setProperty("group.id", parameterTool.get(KAFKA_GROUP_ID, DEFAULT_KAFKA_GROUP_ID));

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);

        DataStream<String> data = env.addSource(kafkaConsumer).uid("kafka-source");

        DataStream<String> filterData = data.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String app = String.valueOf(jsonObject.get("app"));
                return "taobao".equals(app) ? true : false;
            }
        });

        filterData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                
            }
        });


    }
}
