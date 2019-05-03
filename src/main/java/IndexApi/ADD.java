package IndexApi;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

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
    }

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
}
