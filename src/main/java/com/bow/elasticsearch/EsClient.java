package com.bow.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author vv
 * @since 2018/1/27.
 */
public class EsClient {

    //private  EsClient client = new EsClient();
    TransportClient client = null;
    public  EsClient(){
        try{
            Settings settings = Settings.builder()
                    .put("client.transport.sniff", true)
                    .put("cluster.name", "elasticsearch").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        }catch (Exception ex){
            client.close();
        }finally {

        }
    }
    public  TransportClient getConnection(){

        if (client==null){
            synchronized (EsClient.class){
                if (client==null){
                    new EsClient();
                }
            }
        }
        return  client;

    }

}
