package top.xiesen.flink.report.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 样例数据:
 * {"dt","审核时间[年月日 时分秒]","type":"审核类型","username":"审核人员姓名","area":"大区"}
 *
 * @Description 对数据进行清洗
 * @className top.xiesen.flink.report.function.CleanMapFunction
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/13 20:01
 */
public class CleanMapFunction implements MapFunction<String, Tuple3<Long, String, String>> {
    private final Logger logger = LoggerFactory.getLogger(CleanMapFunction.class);

    @Override
    public Tuple3<Long, String, String> map(String value) throws Exception {
        JSONObject jsonObject = JSON.parseObject(value);
        String dt = jsonObject.getString("dt");
        long time = 0L;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date parse = sdf.parse(dt);
            time = parse.getTime();
        } catch (ParseException e) {
            logger.error("时间解析异常,dt: " + dt, e.getCause());
            e.printStackTrace();
        }
        String type = jsonObject.getString("type");
        String area = jsonObject.getString("area");
        return new Tuple3<>(time, type, area);
    }
}
