package com.netcompany.techtalk.batch.config;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;

/** Created by Pawel Gawedzki on 15-Mar-18.*/
@Configuration
public class MongoDBConfig extends AbstractMongoConfiguration {

    @Value("${mongodb.database}")
    private String database;

    @Value("${mongodb.host}")
    private String host;

    @Override
    protected String getDatabaseName() {
        return database;
    }

    @Override
    public MongoClient mongoClient() {
        MongoClientOptions.Builder mongoClientOptionsBuilder = MongoClientOptions.builder()
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .socketKeepAlive(true);
        return new MongoClient(new MongoClientURI(host, mongoClientOptionsBuilder));
    }
}
