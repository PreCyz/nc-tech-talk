package com.netcompany.techtalk.batch.job;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

/** Created by Pawel Gawedzki on 16-Mar-18.*/
@Component
public class JobConfigurer {

    private static final String JOB_NAME = "TECH-TALK";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private Tasklet equipmentCargoStep;
    @Autowired
    private Tasklet haulageEquipmentStep;
    @Autowired
    private Tasklet haulageInfoStep;
    @Autowired
    private Tasklet operationalRouteStep;
    @Autowired
    private Tasklet trackingBookingStep;
    @Autowired
    private Tasklet cargoConditioningStep;
    @Autowired
    private JobExecutionDecider stepDecider;
    @Autowired
    private StepExecutionListener stepExecutionListener;

    public Job synchroniseDatabasesJob() {
        return jobBuilderFactory.get(JOB_NAME)
                .incrementer(parametersIncrementer())
                .preventRestart()
                // '*' means that we do not care if it fails or success, step are independent and should be executed that way
                .start(operationRouteStep()).on("*").to(equipmentCargoStep())
                .from(equipmentCargoStep()).on("*").to(cargoConditioningStep())
                .from(cargoConditioningStep()).on("*").to(haulageInfoStep())
                .from(haulageInfoStep()).on("*").to(haulageEquipmentStep())
                .from(haulageEquipmentStep()).next(stepDecider).on("COMPLETED").to(trackingBookingStep())
                .from(trackingBookingStep()).on("*").end()
                .end()
                .build();
    }

    private JobParametersIncrementer parametersIncrementer() {
        return jobParameters -> {
            if (jobParameters == null || jobParameters.isEmpty()) {
                return new JobParametersBuilder().addDate(com.netcompany.techtalk.batch.job.JobParameter.RUN_ID.key(), new Date(System.currentTimeMillis())).toJobParameters();
            }
            Date id = jobParameters.getDate(com.netcompany.techtalk.batch.job.JobParameter.RUN_ID.key(), new Date(System.currentTimeMillis()));
            return new JobParametersBuilder().addDate(JobParameter.RUN_ID.key(), id).toJobParameters();
        };
    }

    private Step trackingBookingStep() {
        return this.stepBuilderFactory.get(trackingBookingStep.getClass().getSimpleName())
                .tasklet(trackingBookingStep)
                .listener(stepExecutionListener)
                .build();
    }

    private Step operationRouteStep() {
        return this.stepBuilderFactory.get(operationalRouteStep.getClass().getSimpleName())
                .tasklet(operationalRouteStep)
                .listener(stepExecutionListener)
                .build();
    }

    private Step equipmentCargoStep() {
        return this.stepBuilderFactory.get(equipmentCargoStep.getClass().getSimpleName())
                .tasklet(equipmentCargoStep)
                .listener(stepExecutionListener)
                .build();
    }

    private Step cargoConditioningStep() {
        return this.stepBuilderFactory.get(cargoConditioningStep.getClass().getSimpleName())
                .tasklet(cargoConditioningStep)
                .listener(stepExecutionListener)
                .build();
    }

    private Step haulageInfoStep() {
        return this.stepBuilderFactory.get(haulageInfoStep.getClass().getSimpleName())
                .tasklet(haulageInfoStep)
                .listener(stepExecutionListener)
                .build();
    }

    private Step haulageEquipmentStep() {
        return this.stepBuilderFactory.get(haulageEquipmentStep.getClass().getSimpleName())
                .tasklet(haulageEquipmentStep)
                .listener(stepExecutionListener)
                .build();
    }
}
