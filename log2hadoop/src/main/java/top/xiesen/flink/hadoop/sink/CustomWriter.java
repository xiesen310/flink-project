package top.xiesen.flink.hadoop.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import top.xiesen.flink.common.model.LogType;

import java.io.IOException;

/**
 * @Description 自定义写入 Hadoop
 * @className top.xiesen.flink.hadoop.sink.CustomWriter
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 15:08
 */
public class CustomWriter extends StringWriter<LogType> {
    private final String charset = "UTF-8";

    @Override
    public void write(LogType element) throws IOException {
        FSDataOutputStream outputStream = getStream();
        String data = JSON.toJSONString(element);
        outputStream.write(data.getBytes(charset));
        outputStream.write('\n');
    }
}
