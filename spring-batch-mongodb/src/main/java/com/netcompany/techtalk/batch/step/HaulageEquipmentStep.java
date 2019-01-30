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

/** Created by Pawel Gawedzki on 16-Mar-18.*/
@Component
public class HaulageEquipmentStep extends AbstractStep {

    private static final String COLLECTION_NAME = "haulageEquipments";
    public static final String DATA_SET_NAME = "haulage_equipment";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.7fe2b4bc-c60f-4e05-9b36-8f7cd602d5ab";

    @Autowired
    public HaulageEquipmentStep(MongoTemplate mongoTemplate, DataClient dataClient) {
        super(dataClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        MongoCollection<Document> tempDBCollection = getTempDBCollection();

        String indexName = "HAULAGE_ARRANGEMENT_INSTANCE_ID";
        Document index = new Document("HAULAGE_ARRANGEMENT_INSTANCE_ID", 1);
        tempDBCollection.createIndex(index, new IndexOptions().name(indexName).background(true).unique(true));
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForHaulageEquipmentColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(Document document, Date creationDate) {
        return new CreationDetails("HAULAGE_ARRANGEMENT_INSTANCE_ID", String.valueOf(document.get("HAULAGE_ARRANGEMENT_INSTANCE_ID")), creationDate);
    }
}
