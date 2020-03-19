package top.xiesen.flink.common.utils;

import org.joda.time.DateTime;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description 时间处理工具类
 * @className top.xiesen.flink.common.utils.DateUtil
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:27
 */
public class DateUtil {
    private static DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS+08:00");

    private static ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy.MM.dd");
        }
    };
    private static ThreadLocal<SimpleDateFormat> utcSdf = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS+08:00");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf1 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMdd");
        }
    };

    public static Long timestamp(String timestamp) {
        return new DateTime(timestamp).toDate().getTime();
    }

    public static String format(String timestamp) throws ParseException {
        return sdf.get().format(new DateTime(timestamp).toDate());
    }

    public static String getUtcTimeStr() {
        return format.format(new Date()).toString();
    }

    public static String getUtcTimeStr(long interval) {
        long currentTimeMillis = System.currentTimeMillis();
        return format.format(new Date(currentTimeMillis + interval)).toString();
    }

    public static String formatTime(String utcDateStr) {
        Date date = null;
        try {
            date = utcSdf.get().parse(utcDateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return sdf1.get().format(date);
    }
}
