package top.xiesen.clean.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Description 清洗逻辑
 * @className top.xiesen.clean.function.MyCoFlatMapFunction
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 13:47
 */
public class MyCoFlatMapFunction implements CoFlatMapFunction<String, HashMap<String, String>, String> {

    /**
     * 存储国家和大区的映射关系
     */
    HashMap<String, String> allMap = new HashMap<>();

    /**
     * flatMap1 处理的是 kafka 中的数据
     *
     * @param value String
     * @param out   Collector<String>
     * @throws Exception
     */
    @Override
    public void flatMap1(String value, Collector<String> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String dt = jsonObject.getString("dt");
        String countryCode = jsonObject.getString("countryCode");

        // 获取大区
        String area = allMap.get(countryCode);

        JSONArray jsonArray = jsonObject.getJSONArray("data");
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject1 = jsonArray.getJSONObject(i);
            jsonObject1.put("area", area);
            jsonObject1.put("dt", dt);
            out.collect(jsonObject1.toJSONString());
        }
    }

    /**
     * flatMap2 处理的是 redis 中的数据
     *
     * @param value HashMap<String, String>
     * @param out   Collector<String>
     * @throws Exception
     */
    @Override
    public void flatMap2(HashMap<String, String> value, Collector<String> out) throws Exception {
        this.allMap = value;
    }
}
