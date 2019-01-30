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
public class HaulageInfoStep extends AbstractStep {

    private static final String COLLECTION_NAME = "haulageInfo";
    public static final String DATA_SET_NAME = "haulage_info";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.49252e4a-2697-436a-876f-cf73c28d90b9";

    @Autowired
    public HaulageInfoStep(MongoTemplate mongoTemplate, DataClient dataClient) {
        super(dataClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        MongoCollection<Document> tempDBCollection = getTempDBCollection();

        String indexName = "fk_shipment_version_imp_exp_1_direction_1";
        logger.debug("Creating index [{}]", indexName);
        Document index = new Document("FK_SHIPMENT_VERSION_IMP_EXP", 1);
        index.append("DIRECTION", 1);
        tempDBCollection.createIndex(index, new IndexOptions().name(indexName).background(true));
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForHaulageInfoColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(Document document, Date creationDate) {
        return new CreationDetails("FK_SHIPMENT_VERSION_IMP_EXP", String.valueOf(document.get("FK_SHIPMENT_VERSION_IMP_EXP")), creationDate);
    }
}
