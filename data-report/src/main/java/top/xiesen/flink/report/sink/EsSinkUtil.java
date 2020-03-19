package top.xiesen.flink.report.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import top.xiesen.flink.common.constant.PropertiesConstants;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description ElasticsearchSink 二次开发
 * @className top.xiesen.flink.report.sink.EsSink
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/13 20:07
 */
public class EsSinkUtil {

    /**
     * 解析 es 地址
     *
     * @param parameterTool ParameterTool
     * @return List<HttpHost>
     */
    public static List<HttpHost> parseEsUrl(ParameterTool parameterTool) {
        List<HttpHost> httpHosts = new ArrayList<>();
        String urls = parameterTool.get(PropertiesConstants.ELASTICSEARCH_URLS);
        String[] split = urls.split(",");
        HttpHost httpHost = null;
        for (String url : split) {
            URI uri = URI.create(url);
            httpHost = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
            httpHosts.add(httpHost);
        }
        return httpHosts;
    }

    /**
     * @param parameterTool ParameterTool
     * @return
     */
    public static ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> createIndex(ParameterTool parameterTool) {
        List<HttpHost> httpHosts = parseEsUrl(parameterTool);
        ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> esSinkBuilder =
                new ElasticsearchSink.Builder<Tuple4<String, String, String, Long>>(
                        httpHosts,
                        new ElasticsearchSinkFunction<Tuple4<String, String, String, Long>>() {
                            public IndexRequest createIndexRequest(Tuple4<String, String, String, Long> element) {
                                Map<String, Object> json = new HashMap<>(10);
                                json.put("time", element.f0);
                                json.put("type", element.f1);
                                json.put("area", element.f2);
                                json.put("count", element.f3);

                                // 使用 time + type + area 保证 id 唯一
                                String id = element.f0.replace(" ", "_") + "-" + element.f1 + "-" + element.f2;

                                return Requests.indexRequest()
                                        .index("auditindex")
                                        .type("audittype")
                                        .id(id)
                                        .source(json);
                            }

                            @Override
                            public void process(Tuple4<String, String, String, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                                indexer.add(createIndexRequest(element));
                            }
                        }
                );

        // 设置批量写数据的缓冲区大小
        esSinkBuilder.setBulkFlushMaxActions(parameterTool.getInt(PropertiesConstants.ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40));
        return esSinkBuilder;
    }
}
