package top.xiesen.flink.hadoop.utils;

import org.apache.commons.lang.StringUtils;
import top.xiesen.flink.common.model.LogType;
import top.xiesen.flink.common.utils.DateUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description 通过反射处理分区字段
 * @className top.xiesen.flink.hadoop.utils.ReflectionUtil
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 15:04
 */
public class ReflectionUtil {
    /**
     * 构造方法私有
     */
    private ReflectionUtil() {
    }

    /**
     * 根据字段名称获取对象的属性
     *
     * @param fieldName
     * @param target
     * @return
     * @throws Exception
     */
    public static Object getFieldValueByName(String fieldName, Object target) {
        String firstLetter = fieldName.substring(0, 1).toUpperCase();
        String getter = "get" + firstLetter + fieldName.substring(1);
        Object e = null;
        try {
            Method method = target.getClass().getMethod(getter, new Class[0]);
            e = method.invoke(target, new Object[0]);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return e;
    }

    /**
     * 获取所有字段名字
     *
     * @param target
     * @return
     */
    private static String[] getFiledName(Object target) throws Exception {
        Field[] fields = target.getClass().getDeclaredFields();
        String[] fieldNames = new String[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            fieldNames[i] = fields[i].getName();
        }
        return fieldNames;
    }

    /**
     * 获取所有属性的值
     *
     * @param target
     * @return
     * @throws Exception
     */
    private static Object[] getFiledValues(Object target) throws Exception {
        String[] fieldNames = getFiledName(target);
        Object[] value = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            value[i] = getFieldValueByName(fieldNames[i], target);
        }
        return value;
    }


    private static String getMapKey(String s) {
        int startIndex = s.indexOf("[");
        if (startIndex < 0) {
            return null;
        }
        String substr = s.substring(0, startIndex);
        return substr;
    }

    private static String getMapValue(String s) {
        int startIndex = s.indexOf("'");
        if (startIndex < 0) {
            return null;
        }
        String substr = s.substring(startIndex + 1);
        int endIndex = substr.indexOf("'");
        if (endIndex < 0) {
            return null;
        }
        String ret = substr.substring(0, endIndex);
        return ret;
    }

    /**
     * 解析分区数据
     *
     * @param zorkData
     * @param partition
     * @return
     * @throws Exception
     */
    public static String getPartition(LogType zorkData, String partition) {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isNotEmpty(partition)) {
            List<String> list = Arrays.asList(partition.split(","));
            for (int i = 0; i < list.size(); i++) {
                String value = list.get(i);
                if ("timestamp".equals(value)) {
                    String timestamp = (String) getFieldValueByName("timestamp", zorkData);
                    String yyyyMMddSpt = DateUtil.yyyyMMddSpt(timestamp);
                    builder.append("/" + yyyyMMddSpt);
                } else {
                    if (value.contains("[")) {
                        String mapKey = getMapKey(value);
                        Map map = (Map) getFieldValueByName(mapKey, zorkData);
                        Object o = map.get(getMapValue(value));
                        builder.append("/" + o.toString());
                    } else {
                        Object o = getFieldValueByName(value, zorkData);
                        builder.append("/" + o.toString());
                    }
                }

            }
        }
        return builder.toString();
    }

    public static void main(String[] args) throws Exception {
        LogType logType = new LogType();
        logType.setLogTypeName("test");
        logType.setTimestamp("2020-03-11T18:30:21.847+08:00");
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("appsystem", "tdx");
        logType.setDimensions(dimensions);

        Object logTypeName = getFieldValueByName("logTypeName", logType);
        System.out.println("logTypeName = " + logTypeName);

        Object dimensions1 = getFieldValueByName("dimensions", logType);
        System.out.println(dimensions1);
        Map map = (Map) dimensions1;

        System.out.println(map.get("appsystem"));

        String str = "logTypeName,timestamp,dimensions['appsystem']";


        System.out.println(getMapKey("dimensions['appsystem']"));
        System.out.println(getMapValue("dimensions['appsystem']"));


        String partition = getPartition(logType, str);
        System.out.println(partition);


    }
}
