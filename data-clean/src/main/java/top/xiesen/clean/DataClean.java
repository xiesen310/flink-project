package top.xiesen.clean;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.xiesen.clean.function.MyCoFlatMapFunction;
import top.xiesen.clean.sink.KafkaSinkUtil;
import top.xiesen.clean.source.MyKafkaSource;
import top.xiesen.clean.source.MyRedisSource;
import top.xiesen.flink.common.utils.CheckPointUtil;
import top.xiesen.flink.common.utils.ExecutionEnvUtil;

import java.util.HashMap;

/**
 * @Description
 * @className top.xiesen.clean.DataClean
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 13:15
 */
public class DataClean {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env = CheckPointUtil.setCheckpointConfig(env, parameterTool);

        // kafka source
        DataStreamSource<String> data = env.addSource(MyKafkaSource.source(parameterTool));

        // 最新的国家码和大区的映射关系
        DataStream<HashMap<String, String>> mapData = env.addSource(new MyRedisSource()).broadcast();

        // 清洗逻辑
        DataStream<String> resData = data.connect(mapData).flatMap(new MyCoFlatMapFunction());

        // sink
        resData.addSink(KafkaSinkUtil.sink(parameterTool));

        env.execute(DataClean.class.getSimpleName());
    }
}
