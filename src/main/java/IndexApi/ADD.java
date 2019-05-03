package IndexApi;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Auther: dyh
 * Date: 2019/5/3 14:31
 * Description:添加文档
 */
public class ADD {
    public static void main(String[] args) throws IOException {
        addDoc1();
//        bulkDoc();
    }

    /**
     * 插入
     * @return
     * @throws IOException
     */
    public static TransportClient addDoc1() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("brand", "hah")
                .field("color", "yellow")
                .field("model", "l")
                .field("postDate", new Date())
                .endObject();
        String json = Strings.toString(builder);
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("clothes", "young", "2");
        IndexResponse response = indexRequestBuilder.setSource(json, XContentType.JSON).get();
        System.out.println("Index:" + response.getIndex() + "," +
                "Type:" + response.getType() + "," +
                "ID:" + response.getId() + "," +
                "Version:" + response.getVersion() + "," +
                "Status:" + response.status().name()
        );
        return client;
    }

    /**
     * 批量插入
     * @return
     * @throws IOException
     */
    public static TransportClient bulkDoc() throws IOException {
        TransportClient client = TransportClientFactory.getClient();
        BulkRequestBuilder bulk = client.prepareBulk();
        bulk.add(client.prepareIndex("car","model","1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name","保时捷")
                        .field("price","3000w")
                        .field("postDate",new Date())
                        .endObject())

        );
        bulk.add(client.prepareIndex("car", "model", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "法拉利LaFerrari")
                        .field("price", "2250.00万")
                        .field("postDate", new Date())
                        .endObject()
                )
        );
        bulk.add(client.prepareIndex("car", "model", "3")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "法拉利GTC4Lusso")
                        .field("price", "322.80-485.80万")
                        .field("postDate", new Date())
                        .endObject()
                )
        );
        BulkResponse bulkItemResponses = bulk.get();
        RestStatus status = bulkItemResponses.status();
        System.out.println(status.name());
        return client;
    }
}
