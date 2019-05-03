package IndexApi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;

import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 19:21
 * Description:
 */
public class HighQuery
{
    public static void main(String[] args)throws Exception {
//    queryDSL();
//        queryDSL2();
//        queryDSL3();
        queryDSL4();
    }

    public static TransportClient queryDSL() throws UnknownHostException, UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("book")
                .setQuery(QueryBuilders
                        .matchQuery("name", "book_1")
                        .fuzziness(Fuzziness.AUTO)  // 模糊查询
                        .zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL)  // 与MatchAll等价，匹配所有文档。默认none，不匹配任何文档
                ).get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
        return client;
    }

    public static TransportClient queryDSL2() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                // 关键字Aventador，匹配多个字段*ame、brand。字段名称可以使用通配符
                .setQuery(QueryBuilders.multiMatchQuery("Aventador", "*ame","brand"))
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
        return client;
    }

    public static TransportClient queryDSL3() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                // 关键字和字段【均可以】可以使用通配符（?匹配一个字符，*匹配0个或多个字符，AND，OR）等等
                // 有一些您不希望作为操作符的必须转义处理：+ - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /
//                .setQuery(QueryBuilders.queryStringQuery("(book_111) OR (book_999)")) // or
//                .setQuery(QueryBuilders.queryStringQuery("(book_111) AND (book_999)")) // AND
//                .setQuery(QueryBuilders.queryStringQuery("(book_111) && (book_999)")) // AND与&&等价
//                .setQuery(QueryBuilders.queryStringQuery("(book_111) & (book_999)")) // &不会短路计算
//                .setQuery(QueryBuilders.queryStringQuery("book_1?1").field("name")) // ? 并且指定字段
                //.setQuery(QueryBuilders.queryStringQuery("name:book_1?1 OR color:B*"))  // 在查询里指定字段
                //.setQuery(QueryBuilders.queryStringQuery("name:book_1?1 | color:B*"))
                //.setQuery(QueryBuilders.queryStringQuery("name:book_1?1 || color:B*"))  // OR与||等价
                .setQuery(QueryBuilders.queryStringQuery("price:[59000000 TO *]"))  // 范围查询
                // 默认情况下操作符都是可选的，有两个特殊的->首选操作符是：+(这一项必须存在)和-(这一项必须不存在)
//                .setQuery(QueryBuilders.queryStringQuery("price:[990 TO *] -book*"))    // 不显示book*的数据
                .setSize(20)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
        return client;
    }

    public static TransportClient queryDSL4() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("book")
                // + 表示与操作
                // | 表示或操作
                // - 表示否定
                // * 在关键字末尾表示前缀查询
                // 小括号()表示优先级
                // 这里通配符失效！！比如book_1?1或者book_1*1只会查出一条记录
                .setQuery(QueryBuilders.simpleQueryStringQuery("book_11*")
//                        .flags(SimpleQueryStringFlag.AND,SimpleQueryStringFlag.OR,SimpleQueryStringFlag.NOT)  // 指定启用哪些解析功能，默认全部启用ALL。SimpleQueryStringFlag
                )

                .setSize(20)    // 返回数量
                .get();
        searchResponse.getHits().forEach(e -> {
            System.out.println(e.getSourceAsString());
        });
        System.out.println("命中：" + searchResponse.getHits().totalHits);
        return client;
    }
}
