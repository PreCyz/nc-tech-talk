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
public class EquipmentCargoStep extends AbstractStep {

    private static final String COLLECTION_NAME = "equipmentCargo";
    public static final String DATA_SET_NAME = "equipment_cargo";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.0fc9d55a-142e-4385-883d-db1c1a5ef2b4";

    @Autowired
    public EquipmentCargoStep(MongoTemplate mongoTemplate, DataClient dataClient) {
        super(dataClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        MongoCollection<Document> tempDBCollection = getTempDBCollection();

        String indexName = "fk_shipment_version_1";
        logger.debug("Creating index [{}]", indexName);
        Document index = new Document("FK_SHIPMENT_VERSION", 1);
        tempDBCollection.createIndex(index, new IndexOptions().name(indexName).background(true));

        indexName = "equipment_assignment_instance_id_1";
        logger.debug("Creating index [{}]", indexName);
        index = new Document("EQUIPMENT_ASSIGNMENT_INSTANCE_ID", 1);
        tempDBCollection.createIndex(index, new IndexOptions().name(indexName).background(true));
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForEquipmentCargoColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(Document document, Date creationDate) {
        return new CreationDetails("EQUIPMENT_ASSIGNMENT_INSTANCE_ID", String.valueOf(document.get("EQUIPMENT_ASSIGNMENT_INSTANCE_ID")), creationDate);
    }
}
