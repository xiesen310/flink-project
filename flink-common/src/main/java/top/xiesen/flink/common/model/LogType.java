package top.xiesen.flink.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * @Description 日志类型
 * @className top.xiesen.flink.common.model.LogType
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogType implements Serializable {

    private static final long serialVersionUID = 2501833150123699181L;
    /**
     * logTypeName 日志类型
     */
    private String logTypeName;
    /**
     * timestamp 时间戳
     */
    private String timestamp;
    /**
     * source
     */
    private String source;
    /**
     * offset 偏移量
     */
    private String offset;
    /**
     * dimensions 维度
     */
    private Map<String, String> dimensions;
    /**
     * measures
     */
    private Map<String, Double> measures;
    /**
     * normalFields
     */
    private Map<String, String> normalFields;
}
