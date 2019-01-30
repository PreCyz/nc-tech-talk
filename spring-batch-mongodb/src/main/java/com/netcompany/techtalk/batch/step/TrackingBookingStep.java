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
public class TrackingBookingStep extends AbstractStep {

    private static final String COLLECTION_NAME = "truckingBookings";
    public static final String DATA_SET_NAME = "global_bookings_truckinglegs";
    private static final String DATA_SET_RID = "ri.foundry.main.dataset.f6b1f806-8539-49d1-97b6-e262bae8a149";

    @Autowired
    public TrackingBookingStep(MongoTemplate mongoTemplate, DataClient dataClient) {
        super(dataClient, mongoTemplate, DATA_SET_NAME, COLLECTION_NAME, DATA_SET_RID);
    }

    @Override
    protected void createIndexes() {
        MongoCollection<Document> tempDBCollection = getTempDBCollection();

        String indexName = "endLoc_1_depTimeExp_1";
        logger.debug("Creating index [{}]", indexName);
        Document index = new Document("END_LOC", 1);
        index.append("DEP_TIME_EXP", 1);
        tempDBCollection.createIndex(index, new IndexOptions().name(indexName).background(true));

        indexName = "startLoc_1_depTimeExp_1";
        logger.debug("Creating index [{}]", indexName);
        index = new Document("START_LOC", 1);
        index.append("DEP_TIME_EXP", 1);
        tempDBCollection.createIndex(index, new IndexOptions().name(indexName).background(true));

        indexName = "booking_number_1";
        logger.debug("Creating index [{}]", indexName);
        index = new Document("BOOKING_NUMBER", 1);
        tempDBCollection.createIndex(index, new IndexOptions().name(indexName).background(true));
    }

    @Override
    protected QueryColumnBuilder getQueryColumnBuilder(String branchName) {
        return QueryColumnBuilder.queryForGlobalBookingsTruckinglegsColumns(branchName);
    }

    @Override
    protected CreationDetails createCreationDetail(Document document, Date creationDate) {
        return new CreationDetails("BOOKING_NUMBER", String.valueOf(document.get("BOOKING_NUMBER")), creationDate);
    }
}
