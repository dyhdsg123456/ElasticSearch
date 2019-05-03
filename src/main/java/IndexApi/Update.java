package IndexApi;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Auther: dyh
 * Date: 2019/5/3 16:16
 * Description:
 */
public class Update {
    public static void main(String[] args) throws Exception{
//        updateDoc1();
//        updateDoc2();
//        upsert();
        updateByQuery();
    }

    /**
     * 通过UpdateRequest
     * @return
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static TransportClient updateDoc1() throws IOException, ExecutionException, InterruptedException {
        TransportClient client = TransportClientFactory.getClient();
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("car");
        updateRequest.type("model");
        updateRequest.id("3");

        //更新内容
        updateRequest.doc(jsonBuilder().startObject()
                .field("name", "Aventador")
                .field("price", "630.00-755.94万")
                .field("postDate", new Date())
                .field("extra", "Extra Data")   // 不存在的会自动添加
                .endObject());
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println(updateResponse.status().name());
        return client;
    }

    /**
     * 更新方式二：通过prepareUpdate
     * @throws IOException
     */
    public static TransportClient updateDoc2() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        client.prepareUpdate("car", "model", "1")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("name", "法拉利812 Superfast")
                        .field("price", "498.80万")
                        .field("postDate", new Date())
                        .endObject()
                )
                .get();
        return client;
    }

    /**
     * 文档存在则更新doc，不存在则添加upsert
     * 如果文档存在，则只更新model字段，相反会添加IndexRequest里面的内容。
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static TransportClient upsert() throws IOException, ExecutionException, InterruptedException {
        TransportClient client = TransportClientFactory.getClient();
        IndexRequest indexRequest = new IndexRequest("clothes", "young", "3")
                .source(jsonBuilder()
                        .startObject()
                        .field("brand", "Pierre Cardin")
                        .field("color", "Black")
                        .field("model", "L")
                        .field("postDate", new Date())
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest("clothes", "young", "3")
                .doc(jsonBuilder()
                        .startObject()
                        .field("model", "XXXXL")
                        .endObject())
                .upsert(indexRequest);

        UpdateResponse response = client.update(updateRequest).get();
        System.out.println(response.status().name());
        return client;
    }

    /**
     * 当版本匹配时，updateByQuery更新文档并增加版本号。
     * 所有更新和查询失败都会导致updateByQuery中止。这些故障可从BulkByScrollResponse#getBulkFailures方法中获得。
     * 任何成功的更新都会保留并不会回滚。当第一个失败导致中止时，响应包含由失败的批量请求生成的所有失败。
     * 当文档在快照时间和索引请求过程时间之间发生更改时，就会发生版本冲突
     * 为了防止版本冲突导致updateByQuery中止，设置abortOnVersionConflict(false)。
     * ScriptType.INLINE:在大量查询中指定内联脚本并动态编译。它们将基于脚本的lang和代码进行缓存。
     * ScriptType.STORED:存储的脚本作为{@link org.elasticsearch.cluster.ClusterState}的一部分保存基于用户请求。它们将在查询中首次使用时被缓存。
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-update-by-query.html
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html
     * @throws UnknownHostException
     */
    public static TransportClient updateByQuery() throws UnknownHostException, UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery.source("book")
                .size(100)  // 尝试获取的最大文档数
                .filter(QueryBuilders.termsQuery("name","book_233", "book_46", "book_18", "book_512"))  // 注意term，value要变成小写！！
                // 以下脚本：保留id=781的，删除id=316的，其它的价格都变为79
                .script(new Script(
                        ScriptType.INLINE,Script.DEFAULT_SCRIPT_LANG,
                        "if (ctx._source['id'] == 781) {"
                                + "  ctx.op='noop'" // ctx.op='noop'  不做处理
                                + "} else if (ctx._source['id'] == '316') {"
                                + "  ctx.op='delete'"   // ctx.op='delete'删除
                                + "} else {"
                                + "ctx._source['price'] = 79}",
                        Collections.<String, Object>emptyMap()))
                .abortOnVersionConflict(false); // 版本冲突策略：abortOnVersionConflict 版本冲突时不终止
//                .source().setTypes("young") // 指定type
//                .setSize(10)   // 返回搜索的命中数
//                .addSort("postDate", SortOrder.DESC);
        BulkByScrollResponse response = updateByQuery.get();
        System.out.println("Deleted:" + response.getDeleted() + ",Created:" +
                response.getCreated() + ",Updated:" + response.getUpdated() + ",Noops:" + response.getNoops());

        List<BulkItemResponse.Failure> failures = response.getBulkFailures();
        System.out.println(failures.size());
        // 如果目标值是Cat，更新内容也是Cat，则不会去更新
        return client;
    }
}
