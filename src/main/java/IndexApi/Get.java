package IndexApi;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * Auther: dyh
 * Date: 2019/5/3 15:06
 * Description:
 */
public class Get {
    public static void main(String[] args) throws Exception {
//        get();
//        multiGetDoc();
//scrollSearch();
//        search();
        multiSearch();
    }

    private static void get() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        GetResponse response = client.prepareGet("clothes", "young", "2").get();
        String id = response.getId();
        String index = response.getIndex();
        Map<String, DocumentField> fields = response.getFields();
        for (String s : fields.keySet()) {
            DocumentField documentField = fields.get(s);
            String name = documentField.getName();
            List<Object> values = documentField.getValues();
            System.out.println(name + ":" + values.toString());
        }
        // 返回的source，也就是数据源
        Map<String, Object> source = response.getSource();
        System.out.println("==========");

        for(String s : source.keySet()){
            Object o = source.get(s);
            System.out.println(s + ":" + o);
        }
        System.out.println("ID:" + id + ",Index:" + index);
    }
    public static TransportClient multiGetDoc() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet();
        MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.add("clothes", "young", "1", "2")
                .add("car", "model", "1", "2", "3")
                .get();
        for (MultiGetItemResponse response : multiGetItemResponses) {
            GetResponse response1 = response.getResponse();
            if(response1.isExists()){
                String sourceAsString = response1.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }

        return client;
    }

    /**
     * 当搜索请求返回一个结果的“页面”时，滚动API可以用于从一个搜索请求检索大量的结果(甚至所有结果)
     * 其方式与在传统数据库中使用游标非常类似。滚动不是为了实时的用户请求，而是为了处理大量的数据
     * scroll设置的时间，会随着每次的滚屏而刷新。此处不是指整个查询限时为6s，而是到下一次滚屏还有6s的时间。
     * @throws UnknownHostException
     */
    public static void  scrollSearch() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch("book").addSort("price", SortOrder.ASC)
                .setScroll(new TimeValue(6000)).//这批查询在多久时间完成
                        setSize(500).get();// 每次滚出1000条就返回
        while (searchResponse.getHits().getHits().length!=0){
        System.out.println("start===================");
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
            }
            System.out.println("end====================");
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(6000)).execute().actionGet();

    }
    }
    /**
     * 简单查询【通配符查询，筛选价格范围，设定返回数量，排序】
     * @throws UnknownHostException
     */
    public static void search() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        SearchResponse searchResponse = client.prepareSearch("book")// index,可以多个
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.wildcardQuery("name", "*book_1?")) //模糊查询，?匹配单个字符，*匹配多个字符
                .setPostFilter(QueryBuilders.rangeQuery("price").from(500).to(850))
                .setFrom(0).setSize(100)
                .setExplain(true)////explain为true表示根据数据相关度排序，和关键字匹配最高的排在前面
                .addSort("postDate", SortOrder.DESC).get();
        searchResponse.getHits().forEach(e->{
            System.out.println(e.getSourceAsString());
        });


    }

    /**
     * 多个查询
     */
    public static TransportClient multiSearch() throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        // 第一个查询
        SearchRequestBuilder srb1 = client
                .prepareSearch("book")
                .setQuery(QueryBuilders.queryStringQuery("book_9*").field("name"))
                .setFrom(0)    // 开始位置
                .setSize(10);   // 设置返回的最大条数
        // 第二个查询
        SearchRequestBuilder srb2 = client
                .prepareSearch("car")
                .setQuery(QueryBuilders.queryStringQuery("*r*"))
                .setSize(10);
        // 组合
        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .get();

        // You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            response.getHits().forEach(e ->{
                System.out.println(e.getSourceAsString());
            });
            long hits = response.getHits().getTotalHits();
            System.out.println("Hits:" + hits);
            nbHits += hits;
        }
        System.out.println("Total:" + nbHits);
        return client;
    }
}
