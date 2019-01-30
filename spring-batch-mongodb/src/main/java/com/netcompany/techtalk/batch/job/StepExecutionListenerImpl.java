package com.netcompany.techtalk.batch.job;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**Created by Pawel Gawedzki on 04-Jun-2018.*/
@Component("stepExecutionListener")
public class StepExecutionListenerImpl implements StepExecutionListener, StepsStatusContainer {

    private Map<String, ExitStatus> taskStatusMap;

    public StepExecutionListenerImpl() {
        this.taskStatusMap = new HashMap<>();
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {}

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        taskStatusMap.put(stepExecution.getStepName(), stepExecution.getExitStatus());
        return stepExecution.getExitStatus();
    }

    @Override
    public Map<String, ExitStatus> getStepStatusMap() {
        return taskStatusMap;
    }
}
