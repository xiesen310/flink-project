package top.xiesen.warehouse.ods.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description
 * @className top.xiesen.warehouse.ods.model.LogEvent
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 10:48
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogEvent {
    private String collectTime;
    private String source;
    Long offset;
    String message;
    String eventTime;
    String hostname;
}
