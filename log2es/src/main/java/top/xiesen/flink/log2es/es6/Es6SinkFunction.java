package top.xiesen.flink.log2es.es6;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.xiesen.flink.common.constant.LogConstants;
import top.xiesen.flink.common.model.LogType;
import top.xiesen.flink.common.utils.DateUtil;
import top.xiesen.flink.log2es.constant.EsConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @className top.xiesen.flink.log2es.es6.Es6SinkFunction
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:23
 */
public class Es6SinkFunction implements ElasticsearchSinkFunction<LogType> {
    private static final Logger LOG = LoggerFactory.getLogger(Es6SinkFunction.class);
    private static String indexSuffix = "_flink";
    private ParameterTool parameterTool;
    private Map<String, String> indexTopology = new HashMap<>();

    public Es6SinkFunction(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
        parseIndex(parameterTool);
    }

    /**
     * 解析 appsystem 与 index 之间的对象关系
     *
     * @param parameterTool
     */
    private void parseIndex(ParameterTool parameterTool) {
        String indexStr = parameterTool.get(EsConstants.ELASTICSEARCH_INDEX_TOPO).trim();
        String[] indexArray = indexStr.split(EsConstants.REGEX);
        for (String topology : indexArray) {
            String[] temp = topology.split(EsConstants.REGEX1);
            for (String topic : temp[0].split(EsConstants.REGEX2)) {
                indexTopology.put(topic, temp[1].toLowerCase());
            }
        }
    }

    @Override
    public void process(LogType zorkLogData, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        requestIndexer.add(createIndex(zorkLogData));
    }

    /**
     * 创建 index 请求
     *
     * @param logType 日志对象
     * @return IndexRequest
     */
    private IndexRequest createIndex(LogType logType) {
        String logTypeName = null;
        if (logType != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("LogType数据：{}", JSON.toJSON(logType));
            }

            logTypeName = String.valueOf(logType.getLogTypeName());
            String timestamp = String.valueOf(logType.getTimestamp());
            String source = String.valueOf(logType.getSource());
            long offset = 0L;
            try {
                String tmpOffset = String.valueOf(logType.getOffset());
                if (null == tmpOffset || tmpOffset == LogConstants.STRING || tmpOffset.equals(LogConstants.STRING)) {
                    offset = 0L;
                } else {
                    offset = Long.valueOf(tmpOffset);
                }
            } catch (Exception e) {
                LOG.error("获取offset失败,LogType数据: {}", JSON.toJSON(logType), e.getCause());
            }

            Map<String, String> dimensions = new HashMap();
            Map<String, String> tmp = logType.getDimensions();

            try {
                for (Map.Entry entry : tmp.entrySet()) {
                    dimensions.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
            } catch (Exception e) {
                LOG.error("解析 dimensions 出错,LogType数据: {}", JSON.toJSON(logType), e.getCause());
            }

            // 设置 index
            String index = LogConstants.OTHER;
            if (dimensions.containsKey(LogConstants.APPSYSTEM)) {
                if (indexTopology != null) {
                    if (null != indexTopology.get(dimensions.get(LogConstants.APPSYSTEM))) {
                        index = indexTopology.get(dimensions.get(LogConstants.APPSYSTEM));
                    }
                }
            }

            StringBuffer sb = new StringBuffer();
            try {
                sb.append(index);
                sb.append(LogConstants.STR_);
                sb.append(logTypeName);
                sb.append(LogConstants.STR_);
                sb.append(DateUtil.format(timestamp));
                //为新索引添加 flink 后缀
                sb.append(indexSuffix);
            } catch (Exception e) {
                LOG.error("解析 index 出错,index: {}", index, e.getCause());
            }

            String indexStr = sb.toString().toLowerCase();
            Map<String, Double> measures = new HashMap<>();
            Map<String, Double> tmp1 = logType.getMeasures();

            Map<String, String> normalFields = new HashMap<>();
            Map<String, String> tmp2 = logType.getNormalFields();

            try {
                for (Map.Entry entry : tmp1.entrySet()) {
                    measures.put(String.valueOf(entry.getKey()), Double.parseDouble(String.valueOf(entry.getValue())));
                }
                for (Map.Entry entry : tmp2.entrySet()) {
                    normalFields.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
            } catch (Exception e) {
                LOG.error("解析 Measures 和 NormalFields  出错,LogType数据: {}", JSON.toJSON(logType), e.getCause());
            }

            Map<String, Object> bigMap = new HashMap<>();
            bigMap.put(LogConstants.LOG_TYPE_NAME, logTypeName);
            bigMap.put(LogConstants.TIMESTAMP, timestamp);
            bigMap.put(LogConstants.SOURCE, source);
            bigMap.put(LogConstants.OFFSET, offset);
            bigMap.put(LogConstants.DIMENSIONS, dimensions);
            bigMap.put(LogConstants.MEASURES, measures);
            String deserializerTime = new DateTime().toString();
            normalFields.put(LogConstants.DES_TIME, deserializerTime);
            bigMap.put(LogConstants.NORMAL_FIELDS, normalFields);

            LOG.info("正在向ES中写入数据,索引为：{}", indexStr);
            return Requests.indexRequest().index(indexStr).type(logTypeName).source(bigMap, XContentType.JSON);
        } else {
            LOG.error("写入数据失败,错误数据写入到 other index");
            Map<String, Object> bigMap = new HashMap<>();
            return Requests.indexRequest().index(LogConstants.OTHER).type(logTypeName).source(bigMap, XContentType.JSON);
        }
    }
}

