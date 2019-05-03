package IndexApi;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Auther: dyh
 * Date: 2019/5/3 14:39
 * Description:
 */
public class TransportClientFactory {
    private TransportClientFactory(){}
    private static TransportClientFactory transportClientFactory=new TransportClientFactory();
    public static TransportClientFactory getInstance(){
        return transportClientFactory;
    }

    public static TransportClient getClient() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "dyh-es").build();
        return new PreBuiltTransportClient(settings) .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
    }
}
