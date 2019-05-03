package IndexApi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 19:56
 * Description:
 */
public class RangeQuery {
    public static void main(String[] args) throws Exception{
    queryDSL();
    }

    public static void queryDSL() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch().setIndices("school")
                .setQuery(QueryBuilders.rangeQuery("sAge")
//                                .gte(10)    // 大于等于
//                        .gt(10)     // 大于
//                        .lte(20)    // 小于等于
                        .lt(20)     // 小于
                                .from(0)
// .to(5)
                                .includeLower(true)//包含上限
                                .includeUpper(true)//包含下限
                )
                //                .setQuery(QueryBuilders.rangeQuery("sTime")
//                        .lte("now") // 小于等于当前时间
//                        .timeZone("-01:00"))    // 时区
                .setSize(20)//返回数量
                .get();

        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);

    }
}
