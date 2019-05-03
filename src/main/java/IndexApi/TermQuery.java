package IndexApi;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Random;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Auther: dyh
 * Date: 2019/5/3 19:35
 * Description:
 */
public class TermQuery {
    public static void main(String[] args)throws Exception {
//            structuredData();
//        queryDSL();
        queryDSL2();
    }
    /**
     * 准备结构化数据
     * @return
     * @throws IOException
     */
    public static TransportClient structuredData() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        AdminClient admin = client.admin();
        IndicesAdminClient indicesAdminClient = admin.indices();   // 指数管理
        CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate("school")
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 1)   // 分片
                        .put("index.number_of_replicas", 1) // 副本
                )
                .addMapping("student", "sName", "type=text", "sAge", "type=integer",
                        "sClass", "type=keyword", "sTime", "type=date") // mapping
                .get();
        System.out.println("创建索引：" + createIndexResponse.isAcknowledged());
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
                        System.out.println("afterBulk------" + request.getDescription() + ",是否有错误：" + response.hasFailures());
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        //bulk 失败
                        System.out.println("报错-----" + request.getDescription() + "," + failure.getMessage());
                    }
                })
                .setBulkActions(300)  // 每300个request，bulk一次
                .setConcurrentRequests(0)   // 设置并发请求的数量。值为0意味着只允许执行一个请求。值为1意味着在积累新的批量请求时允许执行1个并发请求。
                .build();
        Random random = new Random();
        for (int i = 1; i <= 1000; i++){
            bulkProcessor.add(new IndexRequest("school", "student", i+"").source(jsonBuilder()
                    .startObject()
                    .field("sName", "Student_" + i)
                    .field("sAge", random.nextInt(100))
                    .field("sClass", "Class_" + (i % 20))
                    .field("sTime", new Date())
                    .endObject()));
        }
        bulkProcessor.flush();
        bulkProcessor.close();
        return client;
    }

    public static TransportClient queryDSL() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                /**
                 * 类型为text，会解析其中的内容，比如 "Hey Guy"，倒排索引会包括以下术语[hey,guy]

                 类型为keyword，在倒排索引中会出现以下术语[Hey Guy]

                 所以，查询全文字段就用match，因为它知道如何分析，term比较傻，只知道精确值，擅长结构化的字段。
                 */
//                .setQuery(QueryBuilders.termQuery("sName", "student_1"))    // 使用term查询类型为text的字段的时候，内容要全部小写
                .setQuery(QueryBuilders.termQuery("sClass", "Class_2"))    // 使用term查询类型为keyword的字段的时候，内容区分大小写
                .setSize(20)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
        return client;
    }

    /**
     * 多个查询关键字
     * @return
     * @throws UnknownHostException
     */
    public static TransportClient queryDSL2() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                // 一个字段，多个value
                .setQuery(QueryBuilders.termsQuery("sName","student_1","student_2"))
                .setSize(20)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
        return client;
    }

}
