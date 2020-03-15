package top.xiesen.flink.common.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import top.xiesen.flink.common.model.MetricType;

import java.io.IOException;

/**
 * @Description 指标类型 schema
 * @className top.xiesen.flink.common.schema.MetricTypeSchema
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 13:06
 */
public class MetricTypeSchema<T> implements SerializationSchema<T>, DeserializationSchema<T> {
    private final Class<T> avroType = (Class<T>) MetricType.class;

    @Override
    public T deserialize(byte[] arg0) throws IOException {
        AvroDeserializer metricDeserializer = AvroFactory.getMetricDeserializer();
        GenericRecord genericRecord = metricDeserializer.deserializing(arg0);
        MetricType metricType = JSONObject.parseObject(genericRecord.toString(), MetricType.class);
        return (T) (metricType);
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
        AvroSerializer serializer = AvroFactory.getMetricAvroSerializer();
        return serializer.serializing(JSON.toJSONString(element));
    }
}
