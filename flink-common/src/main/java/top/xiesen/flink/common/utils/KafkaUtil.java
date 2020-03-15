package top.xiesen.flink.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import top.xiesen.flink.common.kafka.SaslConfig;

import javax.security.auth.login.Configuration;
import java.util.Properties;

import static top.xiesen.flink.common.constant.KafkaConstants.*;

/**
 * @Description kafka 工具类
 * @className top.xiesen.flink.common.utils.KafkaUtil
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:45
 */
public class KafkaUtil {
    /**
     * 设置 kafka 消费数据的模式
     *
     * @param kafkaConsumer011   consumer
     * @param kafkaConsumerModel 消费模式
     */
    public static void setKafkaConsumerModel(FlinkKafkaConsumer011 kafkaConsumer011, String kafkaConsumerModel) {
        if ("none".equals(kafkaConsumerModel.toLowerCase())) {
            //  Flink从topic中指定的group上次消费的位置开始消费，所以必须配置group.id参数
            kafkaConsumer011.setStartFromGroupOffsets();
        } else if ("earliest".equals(kafkaConsumerModel.toLowerCase())) {
            // Flink从topic中最初的数据开始消费
            kafkaConsumer011.setStartFromEarliest();
        } else if ("latest".equals(kafkaConsumerModel.toLowerCase())) {
            // Flink从topic中最新的数据开始消费
            kafkaConsumer011.setStartFromLatest();
        }
    }

    /**
     * 获取 kafka 配置信息
     *
     * @return
     */
    public static Properties getKafkaProperties(ParameterTool parameterTool) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.get(KAFKA_BROKERS, DEFAULT_KAFKA_BROKERS));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, parameterTool.get(KAFKA_GROUP_ID, DEFAULT_KAFKA_GROUP_ID));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        String kafkaSecurityModel = parameterTool.get(KAFKA_SECURITY_MODEL, DEFAULT_KAFKA_SECURITY_MODEL);

        if (KAFKA_SECURITY_MODEL_KERBEROS.toLowerCase().equals(kafkaSecurityModel.toLowerCase())) {
            System.setProperty(JAVA_SECURITY_KRB5_CONF, parameterTool.get(JAVA_SECURITY_KRB5_CONF));
            System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, parameterTool.get(JAVA_SECURITY_AUTH_LOGIN_CONFIG));
            properties.put(SECURITY_PROTOCOL, parameterTool.get(SECURITY_PROTOCOL));
            properties.put(SASL_KERBEROS_SERVICE_NAME, parameterTool.get(SASL_KERBEROS_SERVICE_NAME));
            properties.put(SASL_MECHANISM, parameterTool.get(SASL_MECHANISM));
        } else if (KAFKA_SECURITY_MODEL_SASL.toLowerCase().equals(kafkaSecurityModel.toLowerCase())) {
            properties.put(SECURITY_PROTOCOL, parameterTool.get(SECURITY_PROTOCOL));
            properties.put(SASL_MECHANISM, parameterTool.get(SASL_MECHANISM));
            Configuration.setConfiguration(new SaslConfig(parameterTool.get(KAFKA_SASL_APP_KEY), parameterTool.get(KAFKA_SASL_SECRET_KEY)));
        }

        return properties;
    }
}
