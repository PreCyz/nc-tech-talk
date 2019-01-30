package com.netcompany.techtalk.batch.web;

/**Created by Pawel Gawedzki on 23-Jan-2019.*/
public interface DataClient {

    String getSchemaJson(QueryColumnBuilder queryColumnBuilder, String url) throws Exception;
    void downloadDataToFile(UrlBuilder urlBuilder, final String filePath) throws Exception;

}
