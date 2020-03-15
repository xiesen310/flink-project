package top.xiesen.flink.hadoop;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.descriptors.Kafka;
import top.xiesen.flink.common.constant.KafkaConstants;
import top.xiesen.flink.common.constant.TaskConstants;
import top.xiesen.flink.common.model.LogType;
import top.xiesen.flink.common.schema.LogTypeSchema;
import top.xiesen.flink.common.utils.CheckPointUtil;
import top.xiesen.flink.common.utils.ExecutionEnvUtil;
import top.xiesen.flink.common.utils.KafkaUtil;
import top.xiesen.flink.hadoop.sink.HadoopSink;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Description 日志存储到 hadoop
 * @className top.xiesen.flink.hadoop.Log2Hadoop
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 14:56
 */
public class Log2Hadoop {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env = CheckPointUtil.setCheckpointConfig(env, parameterTool);
        env.registerType(LogType.class);

        String sourceName = parameterTool.get(TaskConstants.STREAM_SOURCE_NAME, Kafka.class.getSimpleName().toLowerCase());
        String sinkName = parameterTool.get(TaskConstants.STREAM_SINK_NAME, HadoopFileSystem.class.getSimpleName().toLowerCase());
        String jobName = parameterTool.get(TaskConstants.STREAM_JOB_NAME, Log2Hadoop.class.getSimpleName().toLowerCase());

        String topic = parameterTool.get(KafkaConstants.LOGS_TOPICS);
        List<String> inputTopics = Arrays.asList(topic.split(","));
        Properties prop = KafkaUtil.getKafkaProperties(parameterTool);

        FlinkKafkaConsumer011<LogType> kafkaConsumer011 = new FlinkKafkaConsumer011<>(inputTopics, new LogTypeSchema<LogType>(), prop);
        KafkaUtil.setKafkaConsumerModel(kafkaConsumer011, parameterTool.get(KafkaConstants.KAFKA_CONSUMER_MODEL, KafkaConstants.DEFAULT_KAFKA_CONSUMER_MODEL));

        DataStream<LogType> data = env.addSource(kafkaConsumer011).name(sourceName).uid(sourceName);

        BucketingSink<LogType> sink = HadoopSink.getSink(parameterTool);
        data.addSink(sink).name(sinkName).uid(sinkName);
        env.execute(jobName);
    }

}
