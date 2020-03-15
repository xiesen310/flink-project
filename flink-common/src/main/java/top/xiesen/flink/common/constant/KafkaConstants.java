package top.xiesen.flink.common.constant;

/**
 * @Description
 * @className top.xiesen.flink.common.constant.KafkaConstants
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:48
 */
public class KafkaConstants {
    /**
     * kafka 相关配置
     */
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "flink-project";
    public static final String LOGS_TOPICS = "logs.topics";
    public static final String KAFKA_CONSUMER_MODEL = "kafka.consumer.model";
    public static final String DEFAULT_KAFKA_CONSUMER_MODEL = "latest";

    public static final String KAFKA_SECURITY_MODEL = "kafka.security.model";
    public static final String DEFAULT_KAFKA_SECURITY_MODEL = "none";
    public static final String KAFKA_SECURITY_MODEL_SASL = "sasl";
    public static final String KAFKA_SECURITY_MODEL_KERBEROS = "kerberos";

    /**
     * kafka 认证相关
     */
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    public static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
    public static final String SECURITY_PROTOCOL = "security.protocol";
    public static final String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String KAFKA_SASL_APP_KEY = "kafka.sasl.app.key";
    public static final String KAFKA_SASL_SECRET_KEY = "kafka.sasl.secret.key";
}
