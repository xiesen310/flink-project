package top.xiesen.flink.report.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @Description 过滤异常数据
 * @className top.xiesen.flink.report.filter.MyFilterFunction
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/13 20:04
 */
public class MyFilterFunction implements FilterFunction<Tuple3<Long, String, String>> {
    @Override
    public boolean filter(Tuple3<Long, String, String> value) throws Exception {
        boolean flag = true;
        if (value.f0 == 0L) {
            flag = false;
        }
        return flag;
    }
}
