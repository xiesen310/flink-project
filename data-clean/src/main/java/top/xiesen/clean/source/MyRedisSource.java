package top.xiesen.clean.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import top.xiesen.flink.common.constant.PropertiesConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * redis 中进行数据初始化
 * hset areas AREA_US US
 * hset areas AREA_CT TW,HK
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN IN
 * <p>
 * 需要将大区和国家的对应关系组装成 java 的 HashMap
 *
 * @Description 在 redis 中保存的有国家和大区的关系
 * @className top.xiesen.clean.source.MyRedisSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/14 13:18
 */
public class MyRedisSource extends RichSourceFunction<HashMap<String, String>> {
    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);
    private final long SLEEP_MILLION = 60000;
    private boolean isRunning = true;
    private Jedis jedis = null;
    private String host = null;
    private int port = 6379;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        this.host = params.get(PropertiesConstants.REDIS_HOST);
        this.port = params.getInt(PropertiesConstants.REDIS_PORT);
        this.jedis = new Jedis(host, port);
    }

    @Override
    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {
        // 存储所有国家和大区的关系
        HashMap<String, String> keyValueMap = new HashMap<>();
        while (isRunning) {
            try {
                keyValueMap.clear();
                Map<String, String> areas = jedis.hgetAll("areas");
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        keyValueMap.put(split, key);
                    }
                }
                if (keyValueMap.size() > 0) {
                    ctx.collect(keyValueMap);
                } else {
                    logger.warn("从 redis 中获取的数据为空!");
                }
                Thread.sleep(SLEEP_MILLION);
            } catch (JedisConnectionException e) {
                logger.error("redis 链接异常,重新获取链接 ", e.getCause());
                this.jedis = new Jedis("192.168.0.106", 6379);
            } catch (Exception e) {
                logger.error("redis source 异常 ", e.getCause());
            }
        }
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        if (jedis != null) {
            jedis.close();
        }
        return super.clone();
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (jedis != null) {
            jedis.close();
        }
    }

}
