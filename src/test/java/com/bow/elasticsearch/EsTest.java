package com.bow.elasticsearch;

import java.util.HashMap;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * @author vv
 * @since 2018/1/27.
 */
public class EsTest {

    String index = "crxy";

    String type = "emp";

    TransportClient transportClient;

    /**
     * 相当于初始化方法，在执行其他测试类之前会首先被调用
     * 
     * @throws Exception
     */
    @Before
    public void before() throws Exception {
        transportClient = new EsClient().getConnection();
    }

    /**
     * 自己写测试类的时候可以用这个
     * 
     * @throws Exception
     */
    @Test
    public void test1() throws Exception {

        GetResponse response = transportClient.prepareGet(index, type, "1").execute().actionGet();
        String sourceAsString = response.getSourceAsString();
        System.out.println(sourceAsString);
    }

    /**
     * 工作中使用-1
     * 
     * @throws Exception
     */
    @Test
    public void test2() throws Exception {
        GetResponse response = transportClient.prepareGet(index, type, "1").execute().actionGet();
        String sourceAsString = response.getSourceAsString();
        System.out.println(sourceAsString);
    }


    /**
     * index -json格式
     * 
     * @throws Exception
     */
    @Test
    public void test4() throws Exception {
        String source = "{\"name\":\"mm\",\"age\":\"19\"}";
        IndexResponse response = transportClient.prepareIndex(index, type, "2").setSource(source).execute().actionGet();
        String id = response.getId();
        System.out.println(id);
    }

    /**
     * index - map
     * 
     * @throws Exception
     */
    @Test
    public void test5() throws Exception {
        HashMap<String, Object> source = new HashMap<String, Object>();
        source.put("name", "ww");
        source.put("age", 20);

        IndexResponse response = transportClient.prepareIndex(index, type).setSource(source).execute().actionGet();
        String id = response.getId();
        System.out.println(id);

    }


    /**
     * index -eshelp
     * 
     * @throws Exception
     */
    @Test
    public void test7() throws Exception {
        XContentBuilder endObject = XContentFactory.jsonBuilder().startObject().field("name", "lk").field("age", 28)
                .endObject();
        IndexResponse response = transportClient.prepareIndex(index, type, "5").setSource(endObject).get();
        String id = response.getId();
        System.out.println(id);
    }

    /**
     * get
     * 
     * @throws Exception
     */
    @Test
    public void test8() throws Exception {
        GetResponse response = transportClient.prepareGet(index, type, "5").get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * update
     * 
     * @throws Exception
     */
    @Test
    public void test9() throws Exception {
        XContentBuilder endObject = XContentFactory.jsonBuilder().startObject().field("name", "zs").endObject();

        UpdateResponse response = transportClient.prepareUpdate(index, type, "5").setDoc(endObject).get();
        System.out.println(response.getVersion());

    }

    /**
     * upsert
     * 
     * @throws Exception
     */
    @Test
    public void test10() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index(index);
        request.type(type);
        request.id("6");

        XContentBuilder endObject = XContentFactory.jsonBuilder().startObject().field("name", "aa").endObject();
        request.doc(endObject);

        XContentBuilder endObject2 = XContentFactory.jsonBuilder().startObject().field("name", "crxy").field("age", 10)
                .endObject();
        request.upsert(endObject2);

        UpdateResponse response = transportClient.update(request).get();

        System.out.println(response.getVersion());
    }

    /**
     * 删除
     * 
     * @throws Exception
     */
    @Test
    public void test11() throws Exception {
        DeleteResponse response = transportClient.prepareDelete(index, type, "6").get();
        System.out.println(response.getVersion());
    }



    /**
     * 批量操作 bulk
     * 
     * @throws Exception
     */
    @Test
    public void test13() throws Exception {
        BulkRequestBuilder bulkBuilder = transportClient.prepareBulk();
        IndexRequest indexrequest = new IndexRequest(index, type, "6");
        XContentBuilder endObject = XContentFactory.jsonBuilder().startObject().field("name", "sss")
                .field("age111", 001).endObject();
        indexrequest.source(endObject);
        // TODO---

        bulkBuilder.add(indexrequest);
        DeleteRequest deleteRequest = new DeleteRequest(index, type, "5");
        bulkBuilder.add(deleteRequest);

        BulkResponse bulkResponse = bulkBuilder.get();

        if (bulkResponse.hasFailures()) {
            System.out.println("执行失败：");
            BulkItemResponse[] items = bulkResponse.getItems();
            for (BulkItemResponse bulkItemResponse : items) {
                String failureMessage = bulkItemResponse.getFailureMessage();
                System.out.println(failureMessage);
            }
        } else {
            System.out.println("正常执行");
        }

    }


}
