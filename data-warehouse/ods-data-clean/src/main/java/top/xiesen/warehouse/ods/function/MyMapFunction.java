package top.xiesen.warehouse.ods.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description 逻辑处理
 * @className top.xiesen.warehouse.ods.function.MyMapFunction
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 11:33
 */
public class MyMapFunction implements MapFunction<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(MyMapFunction.class);

    @Override
    public String map(String value) throws Exception {
        JSONObject result = null;
        try {
            JSONObject jsonObject = JSON.parseObject(value);
            String collectTime = String.valueOf(jsonObject.get("@timestamp"));
            String source = String.valueOf(jsonObject.get("source"));
            Long offset = jsonObject.getLong("offset");
            JSONObject fields = JSON.parseObject(String.valueOf(jsonObject.get("fields")));
            String app = String.valueOf(fields.get("app"));

            String message = String.valueOf(jsonObject.get("message"));
            String[] split = message.split("\\|");
            String eventTime = split[0];
            JSONObject jsonMessage = JSON.parseObject(split[1]);

            JSONObject beat = JSON.parseObject(String.valueOf(jsonObject.get("beat")));
            String hostname = String.valueOf(beat.get("hostname"));

            result = new JSONObject();
            result.put("collectTime", collectTime);
            result.put("source", source);
            result.put("offset", offset);
            result.put("message", jsonMessage);
            result.put("eventTime", eventTime);
            result.put("hostname", hostname);
            result.put("odsTime", System.currentTimeMillis());
            result.put("app", app);
        } catch (Exception e) {
            logger.error("逻辑处理异常", e.getCause());
        }
        return result.toJSONString();
    }
}
