package com.netcompany.techtalk.batch.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

import static java.util.stream.Collectors.toMap;

/**Created by Pawel Gawedzki on 04-Jun-2018.*/
@Component
public class StepDecider implements JobExecutionDecider {

    private final Logger logger = LoggerFactory.getLogger(StepDecider.class);
    private final StepsStatusContainer stepsStatusContainer;

    @Autowired
    public StepDecider(@Qualifier("stepExecutionListener") StepsStatusContainer stepsStatusContainer) {
        this.stepsStatusContainer = stepsStatusContainer;
    }

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        Map<String, ExitStatus> taskStatusMap = stepsStatusContainer.getStepStatusMap();
        FlowExecutionStatus flowExecutionStatus = new FlowExecutionStatus(stepExecution.getExitStatus().getExitCode());
        Map<String, String> notCompletedMap = taskStatusMap.entrySet().stream()
                .filter(entry -> ExitStatus.COMPLETED.compareTo(entry.getValue()) != 0)
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().getExitCode()));
        if (!notCompletedMap.isEmpty()) {
            logger.warn("Following steps were not completed: {}", notCompletedMap);
            flowExecutionStatus = FlowExecutionStatus.FAILED;
        }

        return flowExecutionStatus;
    }
}
