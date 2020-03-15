package top.xiesen.flink.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @Description
 * @className top.xiesen.flink.common.model.MetricType
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 13:06
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetricType {
    String metricTypeName;
    String timestamp;
    Map<String, String> dimensions;
    Map<String, String> metrics;
}
