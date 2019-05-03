package IndexApi;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.document.DocumentField;

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
        multiGetDoc();
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

}
