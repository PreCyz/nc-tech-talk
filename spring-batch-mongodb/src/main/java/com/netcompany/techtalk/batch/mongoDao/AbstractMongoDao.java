package com.netcompany.techtalk.batch.mongoDao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/* Created by Pawel Gawedzki on 16-Mar-18.*/
abstract class AbstractMongoDao {
    private static final String SEQUENCES_COLLECTION_NAME = "Sequences";
    private static final String ID_KEY = "_id";
    private static final String NS_KEY = "_ns";

    static final String VERSION_KEY = "version";
    static final String START_TIME_KEY = "startTime";
    static final String END_TIME_KEY = "endTime";
    static final String EXIT_CODE_KEY = "exitCode";
    static final String EXIT_MESSAGE_KEY = "exitMessage";
    static final String LAST_UPDATED_KEY = "lastUpdated";
    static final String STATUS_KEY = "status";
    static final String DOT_ESCAPE_STRING = "\\{dot}";
    static final String DOT_STRING = "\\.";

    // Job Constants
    static final String JOB_INSTANCE_ID_KEY = "jobInstanceId";
    static final String JOB_NAME_KEY = "jobName";
    static final String JOB_PARAMETERS_KEY = "jobParameters";

    // Job Execution Constants
    static final String JOB_EXECUTION_ID_KEY = "jobExecutionId";

    protected Logger logger;

    /**
     * mongoTemplate is used to CRUD Job execution data in Mongo db. This bean
     * needs to be set during bean definition for MongoExecutionContextDao
     */
    @Autowired
    protected MongoTemplate mongoTemplate;

    protected void init() {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    protected abstract MongoCollection<Document> getCollection();

    Long getNextId(String name, MongoTemplate mongoTemplate) {
        MongoCollection<Document> collection = mongoTemplate.getDb().getCollection(SEQUENCES_COLLECTION_NAME);
        Document sequence = new Document("name", name);
        collection.updateOne(sequence, new Document("$inc", new Document("value", 1L)), updateOptions());

        Long nextId = makeList(collection.find(sequence).limit(1)).getFirst().getLong("value");
        return nextId == null ? 0L : nextId;
    }

    void removeSystemFields(Document dbObject) {
        dbObject.remove(ID_KEY);
        dbObject.remove(NS_KEY);
    }

    Document jobInstanceIdDocument(Long id) {
        return new Document(MongoJobInstanceDao.JOB_INSTANCE_ID_KEY, id);
    }

    Document jobExecutionIdDocument(Long id) {
        return new Document(JOB_EXECUTION_ID_KEY, id);
    }

    @SuppressWarnings({"unchecked"})
    JobParameters getJobParameters(Long jobInstanceId, MongoTemplate mongoTemplate) {
        FindIterable<Document> documents = mongoTemplate
                .getCollection(JobInstance.class.getSimpleName())
                .find(jobInstanceIdDocument(jobInstanceId)).limit(1);
        Document jobParamObj = makeList(documents).get(0);

        if (jobParamObj != null && jobParamObj.get(MongoJobInstanceDao.JOB_PARAMETERS_KEY) != null){

            Map<String, ?> jobParamsMap = (Map<String, ?>) jobParamObj.get(MongoJobInstanceDao.JOB_PARAMETERS_KEY);

            Map<String, JobParameter> map = new HashMap<>(jobParamsMap.size());
            for (Map.Entry<String, ?> entry : jobParamsMap.entrySet()) {
                Object param = entry.getValue();
                String key = entry.getKey().replaceAll(DOT_ESCAPE_STRING, DOT_STRING);
                if (param instanceof String) {
                    map.put(key, new JobParameter((String) param));
                } else if (param instanceof Long) {
                    map.put(key, new JobParameter((Long) param));
                } else if (param instanceof Double) {
                    map.put(key, new JobParameter((Double) param));
                } else if (param instanceof Date) {
                    map.put(key, new JobParameter((Date) param));
                } else {
                    map.put(key, null);
                }
            }
            return new JobParameters(map);
        }
        return null;
    }

    <E> LinkedList<E> makeList(Iterable<E> iter) {
        LinkedList<E> list = new LinkedList<>();
        for (E item : iter) {
            list.add(item);
        }
        return list;
    }

    UpdateOptions updateOptions() {
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        return updateOptions;
    }
}
