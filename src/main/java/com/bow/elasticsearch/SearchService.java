package com.bow.elasticsearch;

/**
 * @author vv
 * @since 2018/1/27.
 */
public interface SearchService {

    void search();

    void searchByCondition() throws Exception;

    void multiSearch();

    void aggsearch();

    void metricsAgg();
}
