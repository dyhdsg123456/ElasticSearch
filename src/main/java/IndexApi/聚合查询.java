package IndexApi;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Auther: dyh
 * Date: 2019/5/3 18:32
 * Description:
 */
public class 聚合查询 {
    public static void main(String[] args) throws Exception {
//        aggregationsSearch();
//        metricsAggregationsSearch();
//        geoSearchPreData();
//        geoAggregation();
    bucketAggregationsSearch();
    }
    public static void aggregationsSearch() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch("book")
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(AggregationBuilders.stats("agg1").field("price"))
                .addAggregation(AggregationBuilders.dateHistogram("agg2").field("postDate")
                        //date_histogram是按照时间来构建集合(桶)Buckts的,当我们需要按照时间进行做一些数据统计的时候,就可以使用它来进行时间维度上构建指标分析.
                        .dateHistogramInterval(DateHistogramInterval.YEAR)).
                        get();
        Aggregation agg1 = searchResponse.getAggregations().get("agg1");
        System.out.println(agg1.getClass());    // class org.elasticsearch.search.aggregations.metrics.stats.InternalStats
        Aggregation agg2 = searchResponse.getAggregations().get("agg2");
        System.out.println(agg2.getClass());    // class org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram


    }

    /**
     * metrics聚合
     * 主要为了统计信息
     * org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles
     * org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks
     * org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality
     * 地理位置聚合：https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_metrics_aggregations.html#java-aggs-metrics-geobounds
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_metrics_aggregations.html#java-aggs-metrics-tophits
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_metrics_aggregations.html#java-aggs-metrics-scripted-metric
     * @return
     * @throws UnknownHostException
     */
    public static TransportClient metricsAggregationsSearch() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse sr = client.prepareSearch("book")
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.min("agg1").field("price")
                )
                .addAggregation(
                        AggregationBuilders.max("agg2").field("price")
                )
                .addAggregation(
                        AggregationBuilders.sum("agg3").field("price")
                )
                .addAggregation(
                        AggregationBuilders.avg("agg4").field("price")
                )
                .addAggregation(
                        AggregationBuilders.count("agg5").field("price")
                )
                .addAggregation(
                        AggregationBuilders.stats("agg6").field("price")
                )
                .get();
        Min agg1 = sr.getAggregations().get("agg1");
        Max agg2 = sr.getAggregations().get("agg2");
        Sum agg3 = sr.getAggregations().get("agg3");
        Avg agg4 = sr.getAggregations().get("agg4");
        ValueCount agg5 = sr.getAggregations().get("agg5");
        Stats agg6 = sr.getAggregations().get("agg6");
        System.out.println("Min:" + agg1.getValue() + ",Max:" + agg2.getValue() + ",Sum:" + agg3.getValue() + ",Avg:" + agg4.getValue() + ",Count:" + agg5.getValue() +
                ",Stats:(" + agg6.getMin() + "," + agg6.getMax() + "," + agg6.getSum() + "," + agg6.getAvg() + "," + agg6.getCount() + ")");
        return client;
    }

    /**
     * 准备地理位置信息
     * @return
     * @throws IOException
     */
    public static TransportClient geoSearchPreData() throws IOException, UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        // 建立索引
        CreateIndexResponse indexResponse = client.admin().indices().prepareCreate("area")
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 1)   // 分片
                        .put("index.number_of_replicas", 1) // 副本
                )
                .addMapping("hospital", "message", "type=text", "location", "type=geo_point")
                .get();
        System.out.println("Index:" + indexResponse.index() + ",ACK:" + indexResponse.isAcknowledged());
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        // bulk 执行之前
                        System.out.println("beforeBulk-----" + request.getDescription());
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        // bulk 执行之后
                        System.out.println("afterBulk------" + request.getDescription() + ",hasFailures：" + response.hasFailures());
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        //bulk 失败
                        System.out.println("报错-----" + request.getDescription() + "," + failure.getMessage());
                    }
                })
                .setBulkActions(100)  // 每100个request，bulk一次
                .setConcurrentRequests(0)   // 设置并发请求的数量。值为0意味着只允许执行一个请求。值为1意味着在积累新的批量请求时允许执行1个并发请求。
                .build();
        Random random = new Random();
        for (int i = 1; i <= 200; i++){
            String lo = new DecimalFormat("#.############").format(random.nextDouble() * 100);
            String la = new DecimalFormat("#.############").format(random.nextDouble() * 100);
            bulkProcessor.add(new IndexRequest("area", "hospital", i+"").source(jsonBuilder()
                    .startObject()
                    .field("name", "hospital-" + i)
                    .field("location", lo + "," + la)
                    .endObject()));
        }
        bulkProcessor.flush();
        bulkProcessor.close();
        return client;
    }

    /**
     * 地理信息查询
     * @return
     * @throws UnknownHostException
     */
    public static TransportClient geoAggregation() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse sr = client.prepareSearch("area")
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.geoBounds("agg").field("location").wrapLongitude(true)
                )
                .get();
        GeoBounds agg = sr.getAggregations().get("agg");
        GeoPoint left = agg.topLeft();
        GeoPoint right = agg.bottomRight();
        System.out.println(left + " | " + right);
        return client;
    }

    /**
     * 桶聚合,我这里只列举了部分
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_bucket_aggregations.html
     * @return
     * @throws UnknownHostException
     */
    public static TransportClient bucketAggregationsSearch() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse sr = client.prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
//                .addAggregation(AggregationBuilders
//                        .global("agg0")
//                        .subAggregation(AggregationBuilders.terms("sub_agg").field("name"))
//                )
                .addAggregation(AggregationBuilders
                        .filter("agg1", QueryBuilders.termQuery("name", "book_199")))
                .addAggregation(AggregationBuilders
                        .filters("agg2",
                                new FiltersAggregator.KeyedFilter("key1", QueryBuilders.termQuery("name", "book_1")),
                                new FiltersAggregator.KeyedFilter("key2", QueryBuilders.termQuery("name", "book_52"))
                        ))
                .get();

//        Global agg0 = sr.getAggregations().get("agg0");
//        System.out.println("GlobalCount:" + agg0.getDocCount());

        Filter agg1 = sr.getAggregations().get("agg1");
        System.out.println("FilterCount:" + agg1.getDocCount());

        Filters agg2 = sr.getAggregations().get("agg2");
        for (Filters.Bucket entry : agg2.getBuckets()) {
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
            System.out.println("key [" + key + "], doc_count ["+ docCount +"]");
        }
        return client;
    }
}
