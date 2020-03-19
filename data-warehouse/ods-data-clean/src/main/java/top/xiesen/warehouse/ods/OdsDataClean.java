package top.xiesen.warehouse.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.xiesen.flink.common.utils.CheckPointUtil;
import top.xiesen.flink.common.utils.ExecutionEnvUtil;
import top.xiesen.warehouse.ods.function.MyMapFunction;
import top.xiesen.warehouse.ods.sink.KafkaSinkUtil;
import top.xiesen.warehouse.ods.source.KafkaSourceUtil;

/**
 * @Description 原始数据层(Operational Data Store) 数据清洗
 * @className top.xiesen.warehouse.ods.ODSDataClean
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 10:18
 */
public class OdsDataClean {
    private static final Logger logger = LoggerFactory.getLogger(OdsDataClean.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env = CheckPointUtil.setCheckpointConfig(env, parameterTool);

        // 配置 kafka 数据源
        DataStream<String> data = KafkaSourceUtil.addSource(env, parameterTool);

        DataStream<String> filterData = data.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                String app = null;
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    JSONObject fields = JSON.parseObject(String.valueOf(jsonObject.get("fields")));
                    app = String.valueOf(fields.get("app"));
                } catch (Exception e) {
                    logger.error("获取 app 字段失败");
                }
                return "taobao".equals(app) ? true : false;
            }
        });

        /**
         * 逻辑处理
         */
        DataStream<String> mapData = filterData.map(new MyMapFunction());

        /**
         * 写入 kafka
         */
        mapData.addSink(KafkaSinkUtil.kafkaProducer(parameterTool));

        env.execute(OdsDataClean.class.getSimpleName());
    }
}
