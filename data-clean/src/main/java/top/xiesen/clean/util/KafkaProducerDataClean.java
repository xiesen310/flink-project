package top.xiesen.clean.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 模拟数据格式:
 * {
 * "dt": "2020-02-14 13:24:41",
 * "countryCode": "IN",
 * "data": [{
 * "type": "s1",
 * "score": 0.5,
 * "level": "A+"
 * }, {
 * "type": "s3",
 * "score": 0.2,
 * "level": "D"
 * }]
 * }
 *
 * @Description 模拟数据
 * @className top.xiesen.clean.source.MyRedisSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 13:18
 */
public class KafkaProducerDataClean {

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        //指定kafka broker地址
        prop.put("bootstrap.servers", "s103:9092");
        //指定key value的序列化方式
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        //指定topic名称
        String topic = "allData";

        //创建producer链接
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(prop);

        //{"dt":"2018-01-01 10:11:11","countryCode":"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s2","score":0.2,"level":"B"}]}

        //生产消息
        while (true) {
            String message = "{\"dt\":\"" + getCurrentTime() + "\",\"countryCode\":\"" + getCountryCode() + "\",\"data\":[{\"type\":\"" + getRandomType() + "\",\"score\":" + getRandomScore() + ",\"level\":\"" + getRandomLevel() + "\"},{\"type\":\"" + getRandomType() + "\",\"score\":" + getRandomScore() + ",\"level\":\"" + getRandomLevel() + "\"}]}";
            System.out.println(message);
            producer.send(new ProducerRecord<String, String>(topic, message));
            Thread.sleep(2000);
        }
        //关闭链接
        //producer.close();
    }

    public static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getCountryCode() {
        String[] types = {"US", "TW", "HK", "PK", "KW", "SA", "IN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomType() {
        String[] types = {"s1", "s2", "s3", "s4", "s5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static double getRandomScore() {
        double[] types = {0.3, 0.2, 0.1, 0.5, 0.8};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomLevel() {
        String[] types = {"A", "A+", "B", "C", "D"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


}
