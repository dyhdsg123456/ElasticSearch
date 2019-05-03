package IndexApi;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.document.DocumentField;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * Auther: dyh
 * Date: 2019/5/3 15:06
 * Description:
 */
public class Get {
    public static void main(String[] args) throws UnknownHostException {
        TransportClient client = TransportClientFactory.getClient();
        GetResponse response = client.prepareGet("clothes", "young", "1").get();
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
}
