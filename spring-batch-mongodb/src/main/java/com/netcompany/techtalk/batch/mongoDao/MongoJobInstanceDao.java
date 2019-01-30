package com.netcompany.techtalk.batch.mongoDao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/* Created by Pawel Gawedzki on 16-Mar-18.*/
@Repository
public class MongoJobInstanceDao extends AbstractMongoDao implements JobInstanceDao {

    private static final String JOB_KEY_KEY = "jobKey";

    @PostConstruct
    protected void init() {
        super.init();
        getCollection().createIndex(jobInstanceIdDocument(1L));
    }

    @Override
    public JobInstance createJobInstance(String jobName, final JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");
        Assert.state(getJobInstance(jobName, jobParameters) == null, "JobInstance must not already exist");

        Long jobId = getNextId(JobInstance.class.getSimpleName(), mongoTemplate);

        JobInstance jobInstance = new JobInstance(jobId, jobName);

        jobInstance.incrementVersion();

        Map<String, JobParameter> jobParams = jobParameters.getParameters();
        Map<String, Object> paramMap = new HashMap<>(jobParams.size());
        for (Map.Entry<String, JobParameter> entry : jobParams.entrySet()) {
            paramMap.put(entry.getKey().replaceAll(DOT_STRING, DOT_ESCAPE_STRING), entry.getValue().getValue());
        }
        Document document = new Document()
                .append(JOB_INSTANCE_ID_KEY, jobId)
                .append(JOB_NAME_KEY, jobName)
                .append(JOB_KEY_KEY, createJobKey(jobParameters))
                .append(VERSION_KEY, jobInstance.getVersion())
                .append(JOB_PARAMETERS_KEY, new Document(paramMap));

        getCollection().replaceOne(document, document, updateOptions());

        return jobInstance;
    }

    @Override
    public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        String jobKey = createJobKey(jobParameters);
        Document query = new Document(JOB_NAME_KEY, jobName).append(JOB_KEY_KEY, jobKey);
        LinkedList<Document> documents = makeList(getCollection().find(query));
        Document document = documents.isEmpty() ? null : documents.getFirst();

        return mapJobInstance(document, jobParameters);
    }

    @Override
    public JobInstance getJobInstance(Long instanceId) {
        LinkedList<Document> documents = makeList(getCollection().find(jobInstanceIdDocument(instanceId)));
        Document dbObject = null;
        if (!documents.isEmpty()) {
            dbObject = documents.getFirst();
        }
        return mapJobInstance(dbObject);
    }

    @Override
    public JobInstance getJobInstance(JobExecution jobExecution) {
        Document instanceId = makeList(mongoTemplate.getCollection(JobExecution.class.getSimpleName())
                .find(jobExecutionIdDocument(jobExecution.getId())))
                .getFirst();
        removeSystemFields(instanceId);

        LinkedList<Document> documents = makeList(getCollection().find(instanceId));
        Document document = documents.isEmpty() ? null : documents.getFirst();
        return mapJobInstance(document);
    }

    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        return mapJobInstances(getCollection().find(new Document(JOB_NAME_KEY, jobName)).sort(jobInstanceIdDocument(-1L)).skip(start).limit(count));
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public List<String> getJobNames() {
        List<String> jobNames = makeList(getCollection().distinct(JOB_NAME_KEY, String.class));
        Collections.sort(jobNames);
        return jobNames;
    }

    private String createJobKey(JobParameters jobParameters) {
        Map<String, JobParameter> props = jobParameters.getParameters();
        List<String> keys = new ArrayList<>(props.keySet());
        Collections.sort(keys);

        StringBuilder stringBuilder = new StringBuilder();
        for (String key : keys) {
            stringBuilder.append(key).append("=").append(props.get(key).toString()).append(";");
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available.  Fatal (should be in the JDK).");
        }

        byte[] bytes = digest.digest(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        return String.format("%032x", new BigInteger(1, bytes));
    }

    @Override
    protected MongoCollection<Document> getCollection() {
        return mongoTemplate.getCollection(JobInstance.class.getSimpleName());
    }

    private List<JobInstance> mapJobInstances(FindIterable<Document> documents) {
        return makeList(documents).stream().map(this::mapJobInstance).collect(Collectors.toList());
    }

    private JobInstance mapJobInstance(Document dbObject) {
        return mapJobInstance(dbObject, null);
    }

    private JobInstance mapJobInstance(Document dbObject, JobParameters jobParameters) {
        JobInstance jobInstance = null;
        if (dbObject != null) {
            Long id = (Long) dbObject.get(JOB_INSTANCE_ID_KEY);
            if (jobParameters == null) {
                jobParameters = getJobParameters(id, mongoTemplate);
            }

            jobInstance = new JobInstance(id, (String) dbObject.get(JOB_NAME_KEY)); // should always be at version=0 because they never get updated
            jobInstance.incrementVersion();
        }
        return jobInstance;
    }


    @Override
    public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        List<JobInstance> result = new ArrayList<>();
        List<JobInstance> jobInstances = mapJobInstances(getCollection().find(new Document(JOB_NAME_KEY, jobName)).sort(jobInstanceIdDocument(-1L)));
        for (JobInstance instanceEntry : jobInstances) {
            String key = instanceEntry.getJobName();
            String curJobName = key.substring(0, key.lastIndexOf("|"));

            if (curJobName.equals(jobName)) {
                result.add(instanceEntry);
            }
        }
        return result;
    }

    @Override
    public int getJobInstanceCount(String jobName) throws NoSuchJobException {
        int count = 0;
        List<JobInstance> jobInstances = mapJobInstances(getCollection().find(new Document(JOB_NAME_KEY, jobName)).sort(jobInstanceIdDocument(-1L)));
        for (JobInstance instanceEntry : jobInstances) {
            String key = instanceEntry.getJobName();
            String curJobName = key.substring(0, key.lastIndexOf("|"));

            if (curJobName.equals(jobName)) {
                count++;
            }
        }

        if (count == 0) {
            throw new NoSuchJobException(String.format("No job instances for job name %s were found", jobName));
        } else {
            return count;
        }
    }

}
