package IndexApi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 20:23
 * Description:
 */
public class PrefixQuery {
    public static void main(String[] args)throws Exception {
queryDSL();
    }

    /**
     * 将关键词作为前缀
     * @return
     * @throws UnknownHostException
     */
    public static TransportClient queryDSL() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                .setQuery(QueryBuilders.prefixQuery("sName", "student_19"))
                .setSize(20)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
        return client;
    }
}
