package top.xiesen.flink.common.schema;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;

/**
 * @Description  avro 数据反序列化
 * @className top.xiesen.flink.common.schema.AvroDeserializer
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:53
 */
public class AvroDeserializer {
    private static Logger LOG = Logger.getLogger(AvroDeserializer.class);
    private JSONObject jsonObject;
    private JSONArray jsonArray;
    private Schema schema;
    private String[] keys;

    public AvroDeserializer(String schema) {
        getKeysFromjson(schema);
    }

    /**
     * 获取 avro schema 的 key
     *
     * @param schema schema 字符串
     */
    public void getKeysFromjson(String schema) {
        this.jsonObject = JSONObject.parseObject(schema);
        this.schema = new Schema.Parser().parse(schema);
        this.jsonArray = this.jsonObject.getJSONArray("fields");
        this.keys = new String[this.jsonArray.size()];
        for (int i = 0; i < this.jsonArray.size(); i++) {
            this.keys[i] = this.jsonArray.getJSONObject(i).get("name").toString();
        }
    }

    /**
     * Avro 的反序列化
     *
     * @param body 数：byte[] body：kafka消息
     * @return GenericRecord
     */
    public GenericRecord deserializing(byte[] body) {
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(this.schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(body, null);
        GenericData.Record result = null;
        try {
            result = datumReader.read(null, decoder);
        } catch (Exception e) {
            LOG.error("avro 反序列化失败", e);
        }
        return result;
    }
}
