package top.xiesen.flink.report.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Description
 * @className top.xiesen.flink.report.watermark.MyWatermark
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/12 14:04
 */
public class MyWatermark implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>> {
    long currentMaxTimeStamp = 0L;
    /**
     * 最大允许乱序时间 10s
     */
    final Long maxOutOfOrderness = 10000L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimeStamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
        long timestamp = element.f0;
        currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp);
        return timestamp;
    }
}
