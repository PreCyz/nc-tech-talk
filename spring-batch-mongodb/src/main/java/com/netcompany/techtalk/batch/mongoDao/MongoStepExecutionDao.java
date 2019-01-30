package com.netcompany.techtalk.batch.mongoDao;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.List;

/* Created by Pawel Gawedzki on 16-Mar-18.*/
@Repository
public class MongoStepExecutionDao extends AbstractMongoDao implements StepExecutionDao {

    // Step Execution Constants
    private static final String STEP_EXECUTION_ID_KEY = "stepExecutionId";
    private static final String STEP_NAME_KEY = "stepName";
    private static final String COMMIT_COUNT_KEY = "commitCount";
    private static final String READ_COUNT_KEY = "readCount";
    private static final String FILTER_COUT_KEY = "filterCout";
    private static final String WRITE_COUNT_KEY = "writeCount";
    private static final String READ_SKIP_COUNT_KEY = "readSkipCount";
    private static final String WRITE_SKIP_COUNT_KEY = "writeSkipCount";
    private static final String PROCESS_SKIP_COUT_KEY = "processSkipCout";
    private static final String ROLLBACK_COUNT_KEY = "rollbackCount";

    @PostConstruct
    protected void init() {
        super.init();
        Document document = new Document(STEP_EXECUTION_ID_KEY, 1).append(JOB_EXECUTION_ID_KEY, 1);
        getCollection().createIndex(document);
    }

    @Override
    public void saveStepExecution(StepExecution stepExecution) {
        Assert.isNull(stepExecution.getId(),
                "to-be-saved (not updated) StepExecution can't already have an id assigned");
        Assert.isNull(stepExecution.getVersion(),
                "to-be-saved (not updated) StepExecution can't already have a version assigned");

        validateStepExecution(stepExecution);

        stepExecution.setId(getNextId(StepExecution.class.getSimpleName(), mongoTemplate));
        stepExecution.incrementVersion(); // should be 0 now

        Document object = toDbObjectWithoutVersion(stepExecution).append(VERSION_KEY, stepExecution.getVersion());
        getCollection().replaceOne(object, object, updateOptions());
    }

    private Document toDbObjectWithoutVersion(StepExecution stepExecution) {
        return new Document()
                .append(STEP_EXECUTION_ID_KEY, stepExecution.getId())
                .append(STEP_NAME_KEY, stepExecution.getStepName())
                .append(JOB_EXECUTION_ID_KEY, stepExecution.getJobExecutionId())
                .append(START_TIME_KEY, stepExecution.getStartTime())
                .append(END_TIME_KEY, stepExecution.getEndTime())
                .append(STATUS_KEY, stepExecution.getStatus().toString())
                .append(COMMIT_COUNT_KEY, stepExecution.getCommitCount())
                .append(READ_COUNT_KEY, stepExecution.getReadCount())
                .append(FILTER_COUT_KEY, stepExecution.getFilterCount())
                .append(WRITE_COUNT_KEY, stepExecution.getWriteCount())
                .append(EXIT_CODE_KEY, stepExecution.getExitStatus().getExitCode())
                .append(EXIT_MESSAGE_KEY, stepExecution.getExitStatus().getExitDescription())
                .append(READ_SKIP_COUNT_KEY, stepExecution.getReadSkipCount())
                .append(WRITE_SKIP_COUNT_KEY, stepExecution.getWriteSkipCount())
                .append(PROCESS_SKIP_COUT_KEY, stepExecution.getProcessSkipCount())
                .append(ROLLBACK_COUNT_KEY, stepExecution.getRollbackCount())
                .append(LAST_UPDATED_KEY, stepExecution.getLastUpdated());
    }

    @Override
    public synchronized void updateStepExecution(StepExecution stepExecution) {
        // Attempt to prevent concurrent modification errors by blocking here if
        // someone is already trying to do it.
        Integer currentVersion = stepExecution.getVersion();
        Integer newVersion = currentVersion + 1;

        Document object = toDbObjectWithoutVersion(stepExecution);
        object.append(VERSION_KEY, newVersion);

        Document query = new Document(STEP_EXECUTION_ID_KEY, stepExecution.getId()).append(VERSION_KEY, currentVersion);

        getCollection().replaceOne(query, object);

        stepExecution.incrementVersion();
    }

    @Override
    public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
        Document document = new Document(STEP_EXECUTION_ID_KEY, stepExecutionId).append(JOB_EXECUTION_ID_KEY, jobExecution.getId());
        return createStepExecution(makeList(getCollection().find(document)).getFirst(), jobExecution);
    }

    private StepExecution createStepExecution(Document document, JobExecution jobExecution) {
        if (document == null) {
            return null;
        }
        StepExecution stepExecution = new StepExecution(document.getString(STEP_NAME_KEY), jobExecution, document.getLong(STEP_EXECUTION_ID_KEY));
        stepExecution.setStartTime(document.getDate(START_TIME_KEY));
        stepExecution.setEndTime(document.getDate(END_TIME_KEY));
        stepExecution.setStatus(BatchStatus.valueOf(document.getString(STATUS_KEY)));
        stepExecution.setCommitCount(document.getInteger(COMMIT_COUNT_KEY));
        stepExecution.setReadCount(document.getInteger(READ_COUNT_KEY));
        stepExecution.setFilterCount(document.getInteger(FILTER_COUT_KEY));
        stepExecution.setWriteCount(document.getInteger(WRITE_COUNT_KEY));
        stepExecution.setExitStatus(new ExitStatus(document.getString(EXIT_CODE_KEY), document.getString(EXIT_MESSAGE_KEY)));
        stepExecution.setReadSkipCount(document.getInteger(READ_SKIP_COUNT_KEY));
        stepExecution.setWriteSkipCount(document.getInteger(WRITE_SKIP_COUNT_KEY));
        stepExecution.setProcessSkipCount(document.getInteger(PROCESS_SKIP_COUT_KEY));
        stepExecution.setRollbackCount(document.getInteger(ROLLBACK_COUNT_KEY));
        stepExecution.setLastUpdated(document.getDate(LAST_UPDATED_KEY));
        stepExecution.setVersion(document.getInteger(VERSION_KEY));

        return stepExecution;
    }

    @Override
    public void addStepExecutions(JobExecution jobExecution) {
        Document stepExecutionDocument = new Document(STEP_EXECUTION_ID_KEY, 1L);
        List<Document> documents = makeList(getCollection().find(jobExecutionIdDocument(jobExecution.getId())).sort(stepExecutionDocument));
        documents.forEach(document -> createStepExecution(document, jobExecution));
    }

    @Override
    protected MongoCollection<Document> getCollection() {
        return mongoTemplate.getCollection(StepExecution.class.getSimpleName());
    }

    private void validateStepExecution(StepExecution stepExecution) {
        Assert.notNull(stepExecution, "StepExecution cannot be null.");
        Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
        Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
        Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
    }

    @Override
    public void saveStepExecutions(Collection<StepExecution> stepExecutions) {
        Assert.notNull(stepExecutions, "Attempt to save an null collect of step executions");
        for (StepExecution stepExecution : stepExecutions) {
            saveStepExecution(stepExecution);
        }
    }
}
