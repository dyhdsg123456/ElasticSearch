package IndexApi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 20:31
 * Description:
 */
public class RegexpQuery {
    public static void main(String[] args)throws Exception {
        queryDSL();
    }
    public static void queryDSL() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                // regexp查询的性能在很大程度上取决于所选择的正则表达式。匹配诸如.*之类的所有内容非常慢。*?+ 主要降低性能
                .setQuery(QueryBuilders.regexpQuery("sName", "student_.*0"))
                .setSize(20)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);

    }
}
