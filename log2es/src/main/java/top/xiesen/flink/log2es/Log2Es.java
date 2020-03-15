package top.xiesen.flink.log2es;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.descriptors.Kafka;
import top.xiesen.flink.common.constant.TaskConstants;
import top.xiesen.flink.common.model.LogType;
import top.xiesen.flink.common.schema.LogTypeSchema;
import top.xiesen.flink.common.utils.CheckPointUtil;
import top.xiesen.flink.common.utils.ExecutionEnvUtil;
import top.xiesen.flink.common.utils.KafkaUtil;
import top.xiesen.flink.log2es.es6.Es6Sink;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static top.xiesen.flink.common.constant.KafkaConstants.*;

/**
 * @Description Log2Es 主函数
 * @className top.xiesen.flink.log2es.Log2Es
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:11
 */
public class Log2Es {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env = CheckPointUtil.setCheckpointConfig(env, parameterTool);
        env.registerType(LogType.class);

        String sourceName = parameterTool.get(TaskConstants.STREAM_SOURCE_NAME, Kafka.class.getSimpleName());
        String sinkName = parameterTool.get(TaskConstants.STREAM_SINK_NAME, ElasticsearchSink.class.getSimpleName());
        String jobName = parameterTool.get(TaskConstants.STREAM_JOB_NAME, Log2Es.class.getSimpleName().toLowerCase());

        Properties kafkaProperties = KafkaUtil.getKafkaProperties(parameterTool);
        List<String> inputTopics = Arrays.asList(parameterTool.get(LOGS_TOPICS).split(","));

        FlinkKafkaConsumer011<LogType> kafkaConsumer011 = new FlinkKafkaConsumer011<LogType>(inputTopics, new LogTypeSchema<LogType>(), kafkaProperties);
        KafkaUtil.setKafkaConsumerModel(kafkaConsumer011, parameterTool.get(KAFKA_CONSUMER_MODEL, DEFAULT_KAFKA_CONSUMER_MODEL));
        DataStream<LogType> sources = env.addSource(kafkaConsumer011).name(sourceName).uid(sourceName);

        Es6Sink es6Sink = new Es6Sink(parameterTool);
        ElasticsearchSink es6sink = es6Sink.getElasticsearchSink();

        sources.addSink(es6sink).name(sinkName).uid(sinkName);
        env.execute(jobName);
    }
}
