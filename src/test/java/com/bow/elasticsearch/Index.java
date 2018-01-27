package com.bow.elasticsearch;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * 增删改操作
 */
public class Index {
    private static String index_name = "index_company";

    private static String index_alias = "index_alias_company";

    private static String type_name = "type_company";

    private TransportClient client;

    public void setup() {
        client = new EsClient().getConnection();
    }

    public void execute() throws IOException, ParseException, InterruptedException, ExecutionException {
        // if(index_exists()){
        // delete_index();
        // }
        create_index_type_mapping();
        // String doc_id = post_document();
        // bulk_post_document();
        // update_index("AVXAgZkVZuVe2lbgajEF");
        // bulk_update_index();
        // delete_index("AVXAgZpyZuVe2lbgajEI");
    }

    /**
     * 创建索引
     */
    private void create_index_type_mapping() throws IOException {
        System.out.println("create_index_type_setting_mapping......");

        // 创建数据之前首先创建映射
        XContentBuilder builder_mapping = XContentFactory.jsonBuilder().startObject().startObject(type_name)
                .startObject("properties").startObject("CompanyCode").field("type", "integer")// 还可以指定其他属性：store,indexAnalyzer,searchAnalyzer等等
                .endObject().startObject("SecuCode").field("type", "string").endObject().startObject("ChiName")
                .field("type", "string").endObject().startObject("SecuAbbr").field("type", "string").endObject()
                .startObject("EngName").field("type", "string").endObject().startObject("ChiSpelling")
                .field("type", "string").endObject().startObject("SecuMarket").field("type", "short").endObject()
                .startObject("ListedDate").field("type", "date").endObject().startObject("LastClose")
                .field("type", "double").endObject().endObject().endObject().endObject();

        Map<String, String> settings = new HashMap();
        // 设置分片数还有备份数
        settings.put("number_of_shards", "6");
        settings.put("number_of_replicas", "1");

        // 方式一
        // client.admin().indices().prepareCreate(index_name).execute().actionGet();
        // CreateIndexRequest createIndex =
        // Requests.createIndexRequest(index_name);
        // createIndex.settings(settings).alias(new Alias(index_alias));
        // PutMappingRequest mapping =
        // Requests.putMappingRequest(index_name).type(type_name).source(builder_mapping);
        // client.admin().indices().putMapping(mapping).actionGet();
        // 方式二
        // 查看是否创建好了
        ActionFuture<CreateIndexResponse> response = client.admin().indices()
                .create(Requests.createIndexRequest(index_name).settings(settings).alias(new Alias(index_alias))
                        .mapping(type_name, builder_mapping));
        System.out.println("isAcknowledged: " + response.actionGet().isAcknowledged());

        client.close();
    }

    /**
     * 判断索引是否存在
     */
    private boolean index_exists() {
        System.out.println("index_exists......");
        // 获取es客户端

        ActionFuture<IndicesExistsResponse> response = client.admin().indices()
                .exists(Requests.indicesExistsRequest(index_alias));
        System.out.println("isExists: " + response.actionGet().isExists());

        client.close();

        return response.actionGet().isExists();
    }

    /**
     * 添加索引数据
     * 
     * @throws ParseException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private String post_document() throws ParseException, InterruptedException, ExecutionException {
        System.out.println("post_document......");

        Map<String, Object> map = new HashMap<>();
        // map.put("_id", 3);
        map.put("CompanyCode", 3);
        map.put("SecuCode", "000001");
        map.put("ChiName", "平安银行股份有限公司");
        map.put("SecuAbbr", "平安银行");
        map.put("EngName", "Ping An Bank Co., Ltd.");
        map.put("ChiSpelling", "PAYH");
        map.put("SecuMarket", "90");
        map.put("ListedDate", new SimpleDateFormat("yyyy-MM-dd").parse("1991-04-03"));
        map.put("LastClose", 10.72);

        // ActionFuture<IndexResponse> response =
        // client.index(Requests.indexRequest(index_alias).id("3").type(type_name).source(map));

        ActionFuture<IndexResponse> response = client
                .index(Requests.indexRequest(index_alias).type(type_name).source(map));
        System.out.println("version: " + response.get().getVersion());

        client.close();

        return response.get().getId();
    }

    /**
     * 批量添加索引数据
     * 
     * @throws ParseException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void bulk_post_document() throws ParseException, InterruptedException, ExecutionException {
        System.out.println("bulk_post_document......");

        List<Map<String, Object>> datas = TestDataUtil.getIndexBulkPostData();

        BulkRequest request = Requests.bulkRequest();
        for (Map<String, Object> data : datas) {
            request.add(Requests.indexRequest(index_alias).type(type_name).source(data));
        }
        ActionFuture<BulkResponse> response = client.bulk(request);
        // System.out.println("took time: " + response.get().getTookInMillis());

        client.close();
    }

    /**
     * 删除索引
     */
    private void delete_index() {
        System.out.println("delete_index......");

        ActionFuture<DeleteIndexResponse> response = client.admin().indices()
                .delete(Requests.deleteIndexRequest(index_name));
        // System.out.println("isContextEmpty: " +
        // response.actionGet().isContextEmpty());

        client.close();
    }

    /**
     * 修改文档
     */
    private void update_index(String doc_id) {
        System.out.println("update_index......");

        Map<String, Object> map = new HashMap();
        map.put("ChiSpelling", "PAYH_UPDATE");
        map.put("LastClose", 10.00);
        map.put("TestFeild", "Hello ES!"); // nosql的好处，可以随时增加字段

        UpdateRequestBuilder request = client.prepareUpdate(index_alias, type_name, doc_id).setDoc(map)
                .setDocAsUpsert(true).setFields("_source");

        UpdateResponse response = request.get();
        System.out.println("version: " + response.getVersion());
        System.out.println("updateId: " + response.getGetResult().getId());

        client.close();
    }

    /**
     * 批量修改文档 不过这个批量修改暂时会顶掉其他的字段
     * 
     * @throws ParseException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void bulk_update_index() throws ParseException, InterruptedException, ExecutionException {
        System.out.println("bulk_update_index......");

        BulkRequest request = Requests.bulkRequest();

        Map<String, Object> map = new HashMap();
        map.put("ChiSpelling", "WKA_UPDATE");
        map.put("LastClose", 24.00);
        request.add(Requests.indexRequest(index_alias).type(type_name).id("AVXAgZpyZuVe2lbgajEH").source(map));

        map = new HashMap();
        map.put("ChiSpelling", "SZGT_UPDATE");
        map.put("LastClose", 11.00);
        request.add(Requests.indexRequest(index_alias).type(type_name).id("AVXAgZpyZuVe2lbgajEI").source(map));

        ActionFuture<BulkResponse> response = client.bulk(request);
        System.out.println("updateSize: " + response.get().getItems().length);

        client.close();
    }

    /**
     * 删除指定文档
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void delete_index(String id) throws InterruptedException, ExecutionException {
        System.out.println("delete_index......");

        ActionFuture<DeleteResponse> response = client
                .delete(Requests.deleteRequest(index_alias).type(type_name).id(id));
        // System.out.println("isFound: " + response.get().isFound());

        client.close();
    }

}
