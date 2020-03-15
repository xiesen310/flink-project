package top.xiesen.flink.log2es.constant;

/**
 * @Description
 * @className top.xiesen.flink.log2es.constant.EsConstants
 * @Author 谢森
 * @Email xiesen@zork.com.cn
 * @Date 2020/3/15 12:12
 */
public interface EsConstants {
    String ELASTICSEARCH_URL = "elasticsearch.url";
    String ELASTICSEARCH_CLUSTER_NAME = "elasticsearch.cluster.name";
    String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    String ELASTICSEARCH_BULK_FLUSH_MAX_SIZE_MB = "elasticsearch.bulk.flush.max.size.mb";
    String ELASTICSEARCH_BULK_FLUSH_INTERVAL_MS = "elasticsearch.bulk.flush.interval.ms";
    String ELASTICSEARCH_INDEX_TOPO = "elasticsearch.indexTopo";
    String REGEX = ";";
    String REGEX1 = "=>";
    String REGEX2 = ",";
}
