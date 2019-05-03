package IndexApi;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Random;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Auther: dyh
 * Date: 2019/5/3 14:31
 * Description:添加文档
 */
public class ADD {
    public static void main(String[] args) throws IOException {
//        addDoc1();
//        bulkDoc();
//        scrollSearchPreData();
    }

    /**
     * 插入
     * @return
     * @throws IOException
     */
    public static TransportClient addDoc1() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("brand", "hah")
                .field("color", "yellow")
                .field("model", "l")
                .field("postDate", new Date())
                .endObject();
        String json = Strings.toString(builder);
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("clothes", "young", "2");
        IndexResponse response = indexRequestBuilder.setSource(json, XContentType.JSON).get();
        System.out.println("Index:" + response.getIndex() + "," +
                "Type:" + response.getType() + "," +
                "ID:" + response.getId() + "," +
                "Version:" + response.getVersion() + "," +
                "Status:" + response.status().name()
        );
        return client;
    }

    /**
     * 批量插入
     * @return
     * @throws IOException
     */
    public static TransportClient bulkDoc() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        BulkRequestBuilder bulk = client.prepareBulk();
        bulk.add(client.prepareIndex("car","model","4")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name","保时捷")
                        .field("price","3000w")
                        .field("postDate",new Date())
                        .endObject())

        );
        bulk.add(client.prepareIndex("car", "model", "5")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "法拉利LaFerrari")
                        .field("price", "2250.00万")
                        .field("postDate", new Date())
                        .endObject()
                )
        );
        bulk.add(client.prepareIndex("car", "model", "6")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "法拉利GTC4Lusso")
                        .field("price", "322.80-485.80万")
                        .field("postDate", new Date())
                        .endObject()
                )
        );
        BulkResponse bulkItemResponses = bulk.get();
        RestStatus status = bulkItemResponses.status();
        System.out.println(status.name());
        return client;
    }
    /**
     * .setBulkActions(10000)  // 每10000个request，bulk一次
     .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)) // 每5M的数据刷新一次
     .setFlushInterval(TimeValue.timeValueSeconds(5))    // 每5s刷新一次，而不管有多少数据量
     .setConcurrentRequests(0)   // 设置并发请求的数量。值为0意味着只允许执行一个请求。值为1意味着在积累新的批量请求时允许执行1个并发请求。
     .setBackoffPolicy(  // 设置一个自定义的重试策略，该策略最初将等待100毫秒，按指数增长，最多重试3次。当一个或多个批量项请求失败时，如果出现EsRejectedExecutionException异常，将尝试重试，该异常表明用于处理请求的计算资源太少。要禁用backoff，请传递BackoffPolicy.noBackoff()。
     BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
     */

    public static TransportClient scrollSearchPreData() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        BulkProcessor build = BulkProcessor.builder(client, new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("beforeBulk-----" + request.getDescription());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("afterBulk------" + request.getDescription() + ",是否有错误：" + response.hasFailures());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("报错-----" + request.getDescription() + "," + failure.getMessage());
            }
        }).setBulkActions(100).setConcurrentRequests(0).build();
        Random random = new Random();
        for (int i = 1; i <= 1000; i++) {
           build.add(new IndexRequest("book","elasticsearch",i+"").source(jsonBuilder()
           .startObject().field("name","book_"+i)
           .field("price",random.nextDouble()*1000)
           .field("postDate",new Date())
           .endObject()));
        }
        build.flush();
        build.close();
        return client;
    }

}
