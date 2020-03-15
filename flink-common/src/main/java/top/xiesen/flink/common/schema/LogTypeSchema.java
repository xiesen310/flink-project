package top.xiesen.flink.common.schema;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import top.xiesen.flink.common.model.LogType;

import java.io.IOException;

/**
 * @Description LogTypeSchema
 * @className top.xiesen.flink.common.schema.LogTypeSchema
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 13:02
 */
public class LogTypeSchema<T> implements SerializationSchema<T>, DeserializationSchema<T> {
    private final Class<T> avroType = (Class<T>) LogType.class;

    @Override
    public T deserialize(byte[] arg0) throws IOException {
        AvroDeserializer logsDeserializer = AvroFactory.getLogsDeserializer();
        GenericRecord genericRecord = logsDeserializer.deserializing(arg0);
        LogType logType = JSONObject.parseObject(genericRecord.toString(), LogType.class);
        return (T) (logType);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(avroType);
    }

    @Override
    public byte[] serialize(T element) {
        AvroSerializer avroSerializer = AvroFactory.getLogAvroSerializer();
        return avroSerializer.serializing(avroSerializer.serializing((LogType) element));
    }
}
