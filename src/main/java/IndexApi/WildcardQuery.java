package IndexApi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 20:27
 * Description:
 */
public class WildcardQuery {
    public static void main(String[] args) throws Exception{
queryDSL();
    }
    public static void queryDSL() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch().setIndices("school")
                // 支持通配符查询[?,*]，不建议以通配符开头，那样会很慢
//                .setQuery(QueryBuilders.wildcardQuery("sName", "student_1?1"))
                .setQuery(QueryBuilders.wildcardQuery("sName", "student_1*1"))
                .setSize(20)
                .get();
        searchResponse.getHits().forEach(e->{
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
    }
}
