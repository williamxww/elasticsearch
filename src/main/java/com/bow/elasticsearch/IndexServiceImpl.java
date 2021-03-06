package com.bow.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;

/**
 * @author vv
 * @since 2018/1/27.
 */
public class IndexServiceImpl implements IndexService {

    private EsClient client;

    public void index(String id) {
        // String json = "{" +
        // "\"user\":\"kimchy\"," +
        // "\"postDate\":\"2013-01-30\"," +
        // "\"message\":\"trying out Elasticsearch\"" +
        // "}";
        try {

            Map<String, Object> jsonMap = new HashMap<String, Object>();
            jsonMap.put("name", "jim" + id);
            jsonMap.put("age", 20 + id);
            jsonMap.put("date", new Date());
            jsonMap.put("message", "测试" + id);
            jsonMap.put("tel", "1234567");
            // IndexResponse indexResponse =
            // client.getConnection().prepareIndex("twitter",
            // "tweet").setSource(JSONObject.toJSON(jsonMap),
            // XContentType.JSON).get();
            IndexResponse indexResponse = client.getConnection().prepareIndex("xiaot", "test", id).setSource(jsonMap)
                    .get();
            // Index name
            String _index = indexResponse.getIndex();
            // Type name
            String _type = indexResponse.getType();
            // Document ID (generated or not)
            String _id = indexResponse.getId();
            // Version (if it's the first time you index this document, you will
            // get: 1)
            long _version = indexResponse.getVersion();
            // status has stored current instance statement.
            RestStatus status = indexResponse.status();
            System.out.println(_index + "_" + _type + "_" + _id + "_" + _version + "_" + status);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public void get() {
        GetResponse response = client.getConnection().prepareGet("twitter", "tweet", "94pKEWABJOgzR6sJVCCV").get();
        Map<String, DocumentField> fields = response.getFields();
        System.out.println("map:" + fields);
        String index = response.getIndex();
        Map<String, Object> source = response.getSource();
        String id = response.getId();
        System.out.println(source);

    }

    public void del(String id) {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client.getConnection())
                .filter(QueryBuilders.matchQuery("_id", id)).source("twitter")
                .execute(new ActionListener<BulkByScrollResponse>() {
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                        System.out.println("delete" + deleted);
                    }

                    public void onFailure(Exception e) {

                    }
                });
    }

    public void update(String id) throws Exception {
        client.getConnection().prepareUpdate("twitter", "tweet", id)
                .setDoc(jsonBuilder().startObject().field("name", "tom").endObject()).get();
    }

    public void multiGet(String... ids) throws Exception {
        MultiGetResponse multiGetResponse = client.getConnection().prepareMultiGet().add("twitter", "tweet", ids[0])
                .add("twitter", "tweet", ids[1], ids[2], ids[3]).get();
        for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
            GetResponse response = multiGetItemResponse.getResponse();
            if (response.isExists()) {
                System.out.println(response.getSourceAsString());
            }
        }
    }

    public void bulk(String... ids) throws Exception {
        BulkRequestBuilder prepareBulk = client.getConnection().prepareBulk();
        for (String id : ids) {
            prepareBulk.add(client.getConnection().prepareIndex("twitter", "tweet", id)
                    .setSource(jsonBuilder().startObject().field("name", "肖添" + id).endObject()));

        }
        BulkResponse responses = prepareBulk.get();
        System.out.println(responses.hasFailures());
        for (BulkItemResponse response : responses) {
            System.out.println(response.getResponse().getId() + "," + response.getResponse().getIndex() + ","
                    + response.getResponse().getResult());
        }

    }

    public void bulkProcesstor(String index, String type, String... ids) throws Exception {
        try {

            // IndexResponse indexResponse =
            // client.getConnection().prepareIndex(index,
            // type).setSource(getMapping()).get();
            IndexResponse indexResponse = client.getConnection().prepareIndex(index, type).setSource().get();

            PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type).source(getMapping());
            PutMappingResponse putMappingResponse = client.getConnection().admin().indices().putMapping(mappingRequest)
                    .actionGet();
            // client.getConnection().prepareIndex("temp1","test").
            BulkProcessor bulkProcessor = BulkProcessor.builder(client.getConnection(), new BulkProcessor.Listener() {
                public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                    System.out.println("beforeBulk:" + executionId + "," + bulkRequest.getDescription() + ","
                            + bulkRequest.numberOfActions());
                }

                public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                    System.out.println("afterBulk:" + executionId + "," + bulkRequest.getDescription() + ","
                            + bulkRequest.numberOfActions());
                    System.out.println(
                            "afterBulk:" + executionId + "," + bulkResponse.getItems() + "," + bulkResponse.getTook());
                }

                public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                    System.out.println("afterBulk:" + executionId + "," + bulkRequest.getParentTask() + ","
                            + bulkRequest.getDescription() + "," + throwable);
                }
            }).setBulkActions(10).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)).setConcurrentRequests(1)
                    .setFlushInterval(TimeValue.timeValueMillis(1))
                    .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();

            for (String id : ids) {
                Map<String, Object> jsonMap = new HashMap<String, Object>();
                jsonMap.put("name", "中华人民共和国" + id);
                jsonMap.put("age", 30 + Integer.parseInt(id));
                jsonMap.put("date", new Date());
                jsonMap.put("message", "程序设计" + id);
                jsonMap.put("tel", "18612855433");
                jsonMap.put("attr_name", new String[] { "品牌_sku_attr" + id, "商品类别_sku_attr" + id, "面料_sku_attr" + id });
                jsonMap.put("create_date", new Date());
                bulkProcessor.add(new IndexRequest(index, type, id).source(jsonMap));

            }
            bulkProcessor.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
        } finally {

        }

    }

    private XContentBuilder getMapping() throws Exception {
        XContentBuilder mapping = null;
        try {

            mapping = jsonBuilder().startObject()// .startObject("_ttl").field("enabled",false).endObject()
                    .startObject("properties").startObject("name").field("type", "text")
                    .field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
                    .startObject("age").field("type", "long").endObject().startObject("date").field("type", "date")
                    .endObject().startObject("message").field("type", "keyword").field("index", "true").endObject()
                    .startObject("tel").field("type", "keyword").endObject().startObject("attr_name")
                    .field("type", "keyword").field("index", "true").endObject().endObject().endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return mapping;
    }
}
