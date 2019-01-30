package com.netcompany.techtalk.batch.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.ExecutorService;

/**Created by Pawel Gawedzki on 24-Jan-2019.*/
@Configuration
class ClientConfiguration {

    private enum AppMode {FILE, PALANTIR}

    @Value("${application.mode}")
    private String applicationMode;
    @Value("${palantir.token}")
    private String token;
    @Value("${log.whileWaitingForResponse}")
    private String logWhileWaitingForResponse;
    @Value("${log.wait.interval}")
    private String logWaitIntervalInSeconds;
    @Value("${request.retry.limit}")
    private String requestRetryLimit;

    private RestTemplate restTemplate;
    private ExecutorService executorService;

    @Autowired
    public ClientConfiguration(RestTemplate restTemplate, ExecutorService executorService) {
        this.restTemplate = restTemplate;
        this.executorService = executorService;
    }

    @Bean
    public DataClient clientData() {
        if (applicationMode != null && applicationMode.equalsIgnoreCase(AppMode.FILE.name())) {
            return new FileClient();
        }
        return new PalantirClient(restTemplate, token, executorService, logWhileWaitingForResponse, logWaitIntervalInSeconds, requestRetryLimit);
    }
}
