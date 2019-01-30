package com.netcompany.techtalk.batch.step;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.netcompany.techtalk.batch.data.CreationDetails;
import com.netcompany.techtalk.batch.web.DataClient;
import com.netcompany.techtalk.batch.web.QueryColumnBuilder;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

/** Created by Pawel Gawedzki on 15-Mar-18.*/
@Component
public class OperationalRouteStep extends AbstractStep {

    private static final String COLLECTION_NAME = "operationalRoutes";
    public static final String DATA_SET_NAME = "operational_routes";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.4ef1e435-cb2a-450e-ba18-e42263057379";

    @Autowired
    public OperationalRouteStep(MongoTemplate mongoTemplate, DataClient dataClient) {
        super(dataClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        MongoCollection<Document> tempDBCollection = getTempDBCollection();

        String indexName = "shipment_version_instance_id_1";
        logger.debug("Creating index [{}]", indexName);
        Document index = new Document("SHIPMENT_VERSION_INSTANCE_ID", 1);
        tempDBCollection.createIndex(index, new IndexOptions().name(indexName).background(true));
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForOperationalRoutesColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(Document document, Date creationDate) {
        return new CreationDetails("SHIPMENT_VERSION_INSTANCE_ID", String.valueOf(document.get("SHIPMENT_VERSION_INSTANCE_ID")), creationDate);
    }
}
