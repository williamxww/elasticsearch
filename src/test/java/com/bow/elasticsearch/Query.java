package com.bow.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class Query {
    // private static String index_name = "index_company";
    private static String index_alias = "index_alias_company";

    private static String type_name = "type_company";

    private TransportClient client;

    public void setup() {
        client = new EsClient().getConnection();
    }

    /**
     * 查询所有,可以设置起始终止条数
     */
    public void matchAllQuery() {
        System.out.println("matchAllQuery......");

        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 查询某条数据,可以传入查询参数
     */
    public void matchQuery() {
        System.out.println("matchQuery......");

        QueryBuilder qb = QueryBuilders.matchQuery("LastClose", 3.75);

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 查询某几个字段都包含某个值
     */
    public void multiMatchQuery() {
        System.out.println("multiMatchQuery......");

        String[] queryFields = { "ChiName", "SecuAbbr" };
        QueryBuilder qb = QueryBuilders.multiMatchQuery("北京首钢", queryFields);

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 查询包含某字段的记录
     */
    public void commonTermQuery() {
        System.out.println("commonTermQuery......");

        // 这里是包含 Zhejiang
        QueryBuilder qb = QueryBuilders.commonTermsQuery("EngName", "Zhejiang");

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 指定数值而不是包含的查询
     */
    public void termQuery() {
        System.out.println("termQuery......");

        QueryBuilder qb = QueryBuilders.termQuery("SecuCode", "000002");

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name).setQuery(qb).setFrom(0).setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 查询某个字段在某个范围的记录
     */
    public void rangeQuery() {
        System.out.println("rangeQuery......");

        // 这里可以查询到数据,证明es是可以把string类型的数据读取成数值类型
        QueryBuilder qb = QueryBuilders.rangeQuery("LastClose").gte(1).lt(10);

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 对于字段的各种查询,是否存在 前置属性 后置属性等
     */
    public void existsQuery() {
        System.out.println("existsQuery......");

        QueryBuilder qb = QueryBuilders.existsQuery("CompanyCode");
        // QueryBuilder qb = QueryBuilders.prefixQuery("name", "prefix");
        // QueryBuilder qb = QueryBuilders.wildcardQuery("user", "k?mc*");
        // QueryBuilder qb = QueryBuilders.regexpQuery("user", "k.*y");
        // QueryBuilder qb = QueryBuilders.fuzzyQuery("name", "kimzhy");
        // QueryBuilder qb = QueryBuilders.typeQuery("my_type");
        // QueryBuilder qb =
        // QueryBuilders.idsQuery("my_type","type2").addIds("1","2","5");

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 字段什么开头的模糊查询
     */
    public void prefixQuery() {
        System.out.println("prefixQuery......");

        QueryBuilder qb = QueryBuilders.prefixQuery("SecuCode", "000");

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 占位符模糊查询符合条件的数据
     */
    public void wildcardQuery() {
        System.out.println("wildcardQuery......");

        // ?是匹配一个字符* 是匹配所有字符,并且这里是忽略大小写的
        QueryBuilder qb = QueryBuilders.wildcardQuery("ChiSpelling", "p*y*");

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 通过正则表达式查询符合的数据,正则需要查询资料哦
     */
    public void regexpQuery() {
        System.out.println("regexpQuery......");

        QueryBuilder qb = QueryBuilders.regexpQuery("ChiSpelling", "p.*h");

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 模糊查询--重点:模糊的对象必须是一个单词(前后必须有空格,否则查询不出来)
     */
    public void fuzzyQuery() {
        System.out.println("fuzzyQuery......");

        QueryBuilder qb = QueryBuilders.fuzzyQuery("EngName", "China");

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 查询这个类型下所有的的数据
     */
    public void typeQuery() {
        System.out.println("typeQuery......");

        QueryBuilder qb = QueryBuilders.typeQuery(type_name);

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 根据某几个id进行查询
     */
    public void idsQuery() {
        System.out.println("idsQuery......");

        QueryBuilder qb = QueryBuilders.idsQuery(index_alias, type_name).addIds("AVQUGGZFLd19m5R4UVpv",
                "AVQUGGbRLd19m5R4UVp0");

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 查询分数返回一个恒定的值
     */
    public void constantScoreQuery() {
        System.out.println("constantScoreQuery......");

        // TODO 这个boost不晓得是干什么的
        QueryBuilder qb = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("EngName", "china")).boost(0.6f);

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 相当于 sql语句中的 and or 条件过滤等等
     */
    public void boolQuery() {
        System.out.println("boolQuery......");

        QueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("EngName", "china"))
                .mustNot(QueryBuilders.termQuery("EngName", "company"))
                .should(QueryBuilders.termQuery("EngName", "railway"))
                .filter(QueryBuilders.termQuery("EngName", "technology"));

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 同样是相关与分数的
     */
    // todo 继续看看还不太清楚 boost和 tieBreaker是什么意思
    public void disMaxQuery() {
        System.out.println("disMaxQuery......");

        QueryBuilder qb = QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery("EngName", "china"))
                .add(QueryBuilders.termQuery("EngName", "zhejiang")).boost(1.2f).tieBreaker(0.7f);

        SearchResponse res = client.prepareSearch(index_alias).setTypes(type_name)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setFrom(0).setSize(10).execute()
                .actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

}
