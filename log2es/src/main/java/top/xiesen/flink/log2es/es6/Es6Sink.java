package top.xiesen.flink.log2es.es6;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.xiesen.flink.common.model.LogType;
import top.xiesen.flink.log2es.constant.EsConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @className top.xiesen.flink.log2es.es6.Es6Sink
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:14
 */
public class Es6Sink {
    private static final Logger LOG = LoggerFactory.getLogger(Es6Sink.class);
    private final String commaSeparator = ",";
    private final String colonSeparator = ":";
    private final String httpProtocol = "http";
    private ParameterTool parameterTool;

    public Es6Sink(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;

    }

    public ElasticsearchSink<LogType> getElasticsearchSink() {
        List<HttpHost> httpHosts = new ArrayList<>();

        for (String s : parameterTool.get(EsConstants.ELASTICSEARCH_URL).split(commaSeparator)) {
            String[] httpHost = s.split(colonSeparator);
            httpHosts.add(new HttpHost(httpHost[0], Integer.parseInt(httpHost[1]), httpProtocol));
        }

        /**
         * 组装写入 elasticsearch 数据
         */
        ElasticsearchSinkFunction<LogType> indexLog = new Es6SinkFunction(parameterTool);

        ElasticsearchSink.Builder<LogType> builder = new ElasticsearchSink.Builder<>(httpHosts, indexLog);
        builder.setBulkFlushMaxActions(parameterTool.getInt(EsConstants.ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 3000));
        builder.setBulkFlushMaxSizeMb(parameterTool.getInt(EsConstants.ELASTICSEARCH_BULK_FLUSH_MAX_SIZE_MB, 50));
        builder.setBulkFlushInterval(parameterTool.getLong(EsConstants.ELASTICSEARCH_BULK_FLUSH_INTERVAL_MS, 3000));
        return builder.build();
    }
}
