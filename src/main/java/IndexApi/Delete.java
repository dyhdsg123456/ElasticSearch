package IndexApi;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 15:14
 * Description:
 */
public class Delete {
    public static void main(String[] args) throws Exception {
//            delDoc();
        delDocByQuery();
    }
    /**
     * 根据id删除
     * @throws IOException
     */
    public static TransportClient delDoc() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        DeleteResponse response = client.prepareDelete("clothes", "young", "2").get();
        String id = response.getId();
        String index = response.getIndex();
        String status = response.status().name();
        System.out.println("ID:" + id + ",Index:" + index + ",Status:" + status);
        return client;
    }
    //根据条件删除
    public static TransportClient delDocByQuery() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("brand", "ANTA"))
                .source("clothes")//index
                .get();
//        根据条件删除还可以指定type
//        DeleteByQueryRequestBuilder builder = DeleteByQueryAction.INSTANCE.newRequestBuilder(client);
//        builder.filter(QueryBuilders.matchQuery("brand", "ANTA")) // 属性-值
//                .source("clothes")  // index
//                .source().setTypes("young"); // type
//        BulkByScrollResponse response = builder.get();
        long deleted = bulkByScrollResponse.getDeleted();//删除结果 1成功 0不成功
        System.out.println(deleted);
        return client;
    }
}
