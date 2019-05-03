package IndexApi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 20:39
 * Description:
 */
public class Compoundqueries {
    public static void main(String[] args)throws Exception {
//        constant_score_query();
//        boolquery();
//        queryDSL();
        boosting_query();
    }
    public static void constant_score_query() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                // 通过设置boost提高查询的权重
                .setQuery(QueryBuilders.boolQuery()
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("sName","student_1")).boost(3.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("sName","student_2")).boost(3.3f)))
//                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("sName","student_2")).boost(0.3f)))
                .setSize(10)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println("Score:" + e.getScore() + ",Source:" + e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
    }

    public static void boolquery() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                //查询上下文
                .setQuery(QueryBuilders.boolQuery()
                        // (查询结果)必须出现在匹配的文档中，并将有助于得分
                        .must(QueryBuilders.rangeQuery("sAge").lte(30))
                        // (查询结果)不能出现在匹配的文档中，评分被忽略
                        .mustNot(QueryBuilders.termsQuery("sName", "student_2", "student_4"))
                        // (查询结果)应该出现在匹配的文档中。如果bool查询位于查询上下文中，并且具有must或filter子句，should查询将失效；
                        // 返回的文档可能满足should子句的条件。在一个Bool查询中，如果没有must或者filter，有一个或者多个should子句，那么只要满足一个就可以返回。minimum_should_match参数定义了至少满足几个子句。
                        .should(QueryBuilders.termsQuery("sClass", "Class_1"))
                        // (查询结果)必须出现在匹配的文档中，然而，与must不同的是，查询的分数将被忽略
                        .filter(QueryBuilders.rangeQuery("sAge").gt(5))
                ).setSize(20).get();

        searchResponse.getHits().forEach(e -> {
            System.out.println("Score:" + e.getScore() + ",Source:" + e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
    }

    /**
     * 筛选上下文 query为查询上下文 filter为筛选上下文
     */
    public static TransportClient queryDSL() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.rangeQuery("sAge").gte(10))
                        // 筛选上下文
                        .filter(QueryBuilders.boolQuery()
                                // (查询结果)必须出现在匹配的文档中，并将有助于得分
//                                .must(QueryBuilders.rangeQuery("sAge").lte(30))
                                // (查询结果)不能出现在匹配的文档中，评分被忽略
//                                .mustNot(QueryBuilders.termsQuery("sName","student_21","student_41"))
                                // (查询结果)应该出现在匹配的文档中。如果bool查询位于查询上下文中，并且具有must或filter子句，should查询将失效；
                                // 返回的文档可能满足should子句的条件。在一个Bool查询中，如果没有must或者filter，有一个或者多个should子句，那么只要满足一个就可以返回。minimum_should_match参数定义了至少满足几个子句。
                                        /**
                                         * term仅匹配在给定的字段包含某个词项的文档，如

                                         {“query”:{“term”:{“title”:“crime”}}}
                                         该查询仅匹配在title字段上包含crime的文档

                                         terms允许在给定的字段上包含某些词项的文档，如

                                         {“query”:{“term”:{“title”:[“crime”,"book"],"minimum_match:1}}}

                                         该查询返回在title字段上包含一个或两个被搜索词项的所有文档
                                         */
                                .should(QueryBuilders.termQuery("sClass","Class_1"))
                        ))
                .setSize(500)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println("Score:" + e.getScore() + ",Source:" + e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
        return client;
    }
    public static void boosting_query() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("school")
                // 一种复合查询，分为positive子查询和negitive子查询，两者的查询结构都会返回
                // positive子查询的score保持不变，negative子查询的值将会根据negative_boost的值做相应程度的降低。
                .setQuery(QueryBuilders.boostingQuery(QueryBuilders.termQuery("sName","student_1"),
                        QueryBuilders.termQuery("sClass","Class_1")).negativeBoost(0.3f)
                )
                .setSize(500)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println("Score:" + e.getScore() + ",Source:" + e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
    }

}
