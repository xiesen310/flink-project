package top.xiesen.flink.report;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.xiesen.flink.common.utils.CheckPointUtil;
import top.xiesen.flink.common.utils.ExecutionEnvUtil;
import top.xiesen.flink.report.filter.MyFilterFunction;
import top.xiesen.flink.report.function.CleanMapFunction;
import top.xiesen.flink.report.function.MyAggFunction;
import top.xiesen.flink.report.sink.DelayProcessing;
import top.xiesen.flink.report.sink.EsSinkUtil;
import top.xiesen.flink.report.watermark.MyWatermark;

import java.util.Properties;

import static top.xiesen.flink.common.constant.PropertiesConstants.*;

/**
 * @Description 数据报表主函数
 * @className top.xiesen.report.DataReport
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/12 13:47
 */
public class DataReport {
    private static final Logger logger = LoggerFactory.getLogger(DataReport.class);
    // 保存迟到的数据
    private static OutputTag<Tuple3<Long, String, String>> outputTag = new OutputTag<Tuple3<Long, String, String>>("late-data") {
    };

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env = CheckPointUtil.setCheckpointConfig(env, parameterTool);

        /**
         * 配置 kafka source
         */
        String topic = parameterTool.get(LOGS_TOPIC);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", parameterTool.get(KAFKA_BROKERS, DEFAULT_KAFKA_BROKERS));
        prop.setProperty("group.id", parameterTool.get(KAFKA_GROUP_ID, DEFAULT_KAFKA_GROUP_ID));

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        /**
         * 获取 kafka 中的数据
         * 审核数据格式
         * {"dt","审核时间[年月日 时分秒]","type":"审核类型","username":"审核人员姓名","area":"大区"}
         */
        DataStream<String> data = env.addSource(kafkaConsumer).uid("kafka-source");

        /**
         * 对数据进行清洗
         */
        DataStream<Tuple3<Long, String, String>> mapData = data.map(new CleanMapFunction()).uid("data-clean");

        /**
         * 过滤异常数据
         */
        DataStream<Tuple3<Long, String, String>> filterData = mapData.filter(new MyFilterFunction()).uid("filter-data");

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> resultData = filterData
                .assignTimestampsAndWatermarks(new MyWatermark())
                .keyBy(1, 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .allowedLateness(Time.seconds(15))// 允许迟到 30s
                .sideOutputLateData(outputTag) // 记录迟到太久的数据
                .apply(new MyAggFunction())
                .uid("window-operator");

        // 获取迟到太久的数据
        DataStream<Tuple3<Long, String, String>> sideOutput = resultData.getSideOutput(outputTag);

        /**
         * 把迟到的数据存储到 kafka 中
         */
        DelayProcessing.process(parameterTool, sideOutput);

        /**
         * 把计算的结果存储到 es 中
         */
        resultData.addSink(EsSinkUtil.createIndex(parameterTool).build()).uid("es-sink");

        env.execute(DataReport.class.getSimpleName());
    }
}
