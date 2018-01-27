package com.bow.elasticsearch;

/**
 * @author vv
 * @since 2018/1/27.
 */
public interface IndexService {

    void index(String id);

    void get();

    void del(String id);

    void update(String id) throws Exception;

    void multiGet(String... ids) throws Exception;

    void bulk(String... ids) throws Exception;

    void bulkProcesstor(String index, String type, String... ids) throws Exception;
}
