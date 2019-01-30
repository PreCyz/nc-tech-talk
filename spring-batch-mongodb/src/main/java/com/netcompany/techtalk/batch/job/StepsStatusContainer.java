package com.netcompany.techtalk.batch.job;

import org.springframework.batch.core.ExitStatus;

import java.util.Map;

/**Created by Pawel Gawedzki on 04-Jun-2018.*/
@FunctionalInterface
public interface StepsStatusContainer {
    Map<String, ExitStatus> getStepStatusMap();
}
