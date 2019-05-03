package IndexApi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 20:19
 * Description:
 */
public class ExistQuery {
    public static void main(String[] args)throws Exception {
queryDSL();

    }

    /**
     * ：对于某个字段，返回至少含有一个非空值的所有文档
     * 有空值，null，空数组，非查询字段不返回
     *
     * @throws UnknownHostException
     */
    public static void queryDSL() throws  UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                .setQuery(QueryBuilders.existsQuery("sName"))
                .setSize(20)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);

    }
}
