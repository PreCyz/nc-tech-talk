package com.netcompany.techtalk.batch.mongoDao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/* Created by Pawel Gawedzki on 16-Mar-18.*/
@Repository
public class MongoJobExecutionDao extends AbstractMongoDao implements JobExecutionDao {

    private static final String CREATE_TIME_KEY = "createTime";

    @PostConstruct
    protected void init() {
        super.init();
        Document document = new Document(JOB_EXECUTION_ID_KEY, 1).append(JOB_INSTANCE_ID_KEY, 1);
        getCollection().createIndex(document);
    }

    @Override
    public void saveJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        jobExecution.incrementVersion();
        Long id = getNextId(JobExecution.class.getSimpleName(), mongoTemplate);
        save(jobExecution, id);
    }

    private void save(JobExecution jobExecution, Long id) {
        jobExecution.setId(id);

        Document object = toDbObjectWithoutVersion(jobExecution);
        object.append(VERSION_KEY, jobExecution.getVersion());

        getCollection().replaceOne(object, object, updateOptions());
    }

    private Document toDbObjectWithoutVersion(JobExecution jobExecution) {
        return new Document(JOB_EXECUTION_ID_KEY, jobExecution.getId())
                .append(JOB_INSTANCE_ID_KEY, jobExecution.getJobId())
                .append(START_TIME_KEY, jobExecution.getStartTime())
                .append(END_TIME_KEY, jobExecution.getEndTime())
                .append(STATUS_KEY, jobExecution.getStatus().toString())
                .append(EXIT_CODE_KEY, jobExecution.getExitStatus().getExitCode())
                .append(EXIT_MESSAGE_KEY, jobExecution.getExitStatus().getExitDescription())
                .append(CREATE_TIME_KEY, jobExecution.getCreateTime())
                .append(LAST_UPDATED_KEY, jobExecution.getLastUpdated());
    }

    private void validateJobExecution(JobExecution jobExecution) {
        Assert.notNull(jobExecution, "JobExecution cannot be null.");
        Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
        Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
        Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
    }

    @Override
    public synchronized void updateJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);

        Long jobExecutionId = jobExecution.getId();
        Assert.notNull(jobExecutionId, "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");
        Assert.notNull(jobExecution.getVersion(), "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

        Integer version = jobExecution.getVersion() + 1;

        List<Document> documents = makeList(getCollection().find(jobExecutionIdDocument(jobExecutionId)));
        if (documents.isEmpty()) {
            throw new NoSuchObjectException(String.format("Invalid JobExecution, ID %s not found.", jobExecutionId));
        }

        Document object = toDbObjectWithoutVersion(jobExecution).append(VERSION_KEY, version);
        Document query = new Document()
                .append(JOB_EXECUTION_ID_KEY, jobExecutionId)
                .append(VERSION_KEY, jobExecution.getVersion());

        getCollection().replaceOne(query, object);

        jobExecution.incrementVersion();
    }

    @Override
    public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
        Assert.notNull(jobInstance, "Job cannot be null.");
        Long id = jobInstance.getId();
        Assert.notNull(id, "Job Id cannot be null.");
        List<Document> documents = makeList(getCollection().find(jobInstanceIdDocument(id)).sort(new Document(JOB_EXECUTION_ID_KEY, -1)));
        return documents.stream().map(document -> mapJobExecution(jobInstance, document)).collect(toList());
    }

    @Override
    public JobExecution getLastJobExecution(JobInstance jobInstance) {
        Long id = jobInstance.getId();

        List<Document> documents = makeList(getCollection().find(jobInstanceIdDocument(id)).sort(new Document(CREATE_TIME_KEY, -1)).limit(1));
        if (documents == null || documents.isEmpty()) {
            return null;
        } else {
            if (documents.size() > 1) {
                throw new IllegalStateException("There must be at most one latest job execution");
            }
            Document singleResult = documents.get(0);
            return mapJobExecution(jobInstance, singleResult);
        }
    }

    @Override
    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        FindIterable<Document> instancesCursor = mongoTemplate.getCollection(JobInstance.class.getSimpleName())
                .find(new Document(JOB_NAME_KEY, jobName));
        List<Long> ids = makeList(instancesCursor).stream().map(document -> document.getLong(JOB_INSTANCE_ID_KEY)).collect(toList());

        Document query = new Document(JOB_INSTANCE_ID_KEY, new Document("$in", ids)).append(END_TIME_KEY, null);
        FindIterable<Document> documentIterator = getCollection().find(new Document(query)).sort(jobExecutionIdDocument(-1L));
        return makeList(documentIterator).stream().map(this::mapJobExecution).collect(toSet());
    }

    @Override
    public JobExecution getJobExecution(Long executionId) {
        FindIterable<Document> documents = getCollection().find(jobExecutionIdDocument(executionId));
        return mapJobExecution(makeList(documents).get(0));
    }

    @Override
    public void synchronizeStatus(JobExecution jobExecution) {
        Long id = jobExecution.getId();
        Document jobExecutionObject = makeList(getCollection().find(jobExecutionIdDocument(id))).get(0);
        int currentVersion = jobExecutionObject != null ? ((Integer) jobExecutionObject.get(VERSION_KEY)) : 0;
        if (currentVersion != jobExecution.getVersion()) {
            if (jobExecutionObject == null) {
                save(jobExecution, id);
                jobExecutionObject = makeList(getCollection().find(jobExecutionIdDocument(id))).get(0);
            }
            String status = (String) jobExecutionObject.get(STATUS_KEY);
            jobExecution.upgradeStatus(BatchStatus.valueOf(status));
            jobExecution.setVersion(currentVersion);
        }
    }

    @Override
    protected MongoCollection<Document> getCollection() {
        return mongoTemplate.getCollection(JobExecution.class.getSimpleName());
    }

    private JobExecution mapJobExecution(Document document) {
        return mapJobExecution(null, document);
    }

    private JobExecution mapJobExecution(JobInstance jobInstance, Document document) {
        if (document == null) {
            return null;
        }
        Long id = document.getLong(JOB_EXECUTION_ID_KEY);
        JobExecution jobExecution;

        if (jobInstance == null) {
            jobExecution = new JobExecution(id);
        } else {
            JobParameters jobParameters = getJobParameters(jobInstance.getId(), mongoTemplate);
            jobExecution = new JobExecution(jobInstance, id, jobParameters, null);
        }
        jobExecution.setStartTime(document.getDate(START_TIME_KEY));
        jobExecution.setEndTime(document.getDate(END_TIME_KEY));
        jobExecution.setStatus(BatchStatus.valueOf(document.getString(STATUS_KEY)));
        jobExecution.setExitStatus(new ExitStatus((document.getString(EXIT_CODE_KEY)), document.getString(EXIT_MESSAGE_KEY)));
        jobExecution.setCreateTime(document.getDate(CREATE_TIME_KEY));
        jobExecution.setLastUpdated(document.getDate(LAST_UPDATED_KEY));
        jobExecution.setVersion(document.getInteger(VERSION_KEY));

        return jobExecution;
    }
}
