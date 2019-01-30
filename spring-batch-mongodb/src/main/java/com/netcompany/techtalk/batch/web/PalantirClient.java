package com.netcompany.techtalk.batch.web;

import com.netcompany.techtalk.batch.util.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**Created by Pawel Gawedzki on 9/21/2017.*/
class PalantirClient implements DataClient {

    private static final Logger logger = LoggerFactory.getLogger(PalantirClient.class);

    private final RestTemplate restTemplate;
    private final String token;
    private final ExecutorService executorService;
    private final boolean logWhileWaitingForResponse;
    private final String logWaitIntervalInSeconds;
    private final String requestRetryLimit;

    public PalantirClient(RestTemplate restTemplate, String token, ExecutorService executorService, String logWhileWaitingForResponse,
                          String logWaitIntervalInSeconds, String requestRetryLimit) {
        this.restTemplate = restTemplate;
        this.token = token;
        this.executorService = executorService;
        this.logWhileWaitingForResponse = "true".equals(logWhileWaitingForResponse);
        this.logWaitIntervalInSeconds = logWaitIntervalInSeconds;
        this.requestRetryLimit = requestRetryLimit;
    }

    private int getRequestRetryLimitInt() {
        int result = 1;
        try {
            result = Integer.parseInt(requestRetryLimit);
        } catch (NumberFormatException ex) {
            logger.warn("Wrong value for step.retry.limit [{}]. Default value for it is 1.", requestRetryLimit);
        }
        return result;
    }

    public String getSchemaJson(QueryColumnBuilder queryColumnBuilder, String url) throws Exception {
        validateToken();
        HttpEntity<String> entity = new HttpEntity<>(queryColumnBuilder.build(), createHeadersWithMediaType());

        int attempt = 1;
        do {
            try {
                logger.info("Attempt {}. REST Request sent to [{}] with details [{}].", attempt, url, queryColumnBuilder.toString());
                if (logWhileWaitingForResponse) {
                    String subject = "POST to";
                    ResponseEntity<String> responseEntity = logWhileWaiting(subject, url, () -> restTemplate.postForEntity(url, entity, String.class));
                    return responseEntity.getBody();
                } else {
                    return restTemplate.postForEntity(url, entity, String.class).getBody();
                }
            } catch (Exception ex) {
                logger.error("Error while waiting for response from Palantir.", ex);
                attempt++;
            }
        } while (attempt <= getRequestRetryLimitInt());

        String errMsg = String.format("[%d] attempts. Unable to get the data from [%s].", attempt - 1, url);
        logger.error(errMsg);
        throw new Exception(errMsg);
    }

    private void validateToken() {
        if (StringUtils.nullOrEmpty(token)) {
            throw new RuntimeException("Palantir token is not specified. Please update application.properties with proper value for the key 'gcss.token'.");
        }
    }

    private <T> T logWhileWaiting(String subject, String url, Callable<T> callable) {
        try {
            Future<T> future = executorService.submit(callable);
            long interval = getIntervalInSeconds();
            while (!future.isDone()) {
                logger.debug("Waiting to finish the {} [{}]", subject, url);
                Thread.sleep(interval);
            }
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof HttpServerErrorException) {
                HttpServerErrorException error = (HttpServerErrorException) e.getCause();
                logger.error("HttpServerErrorException details: statusCode: {}, statusText {}, responseBody: {}",
                        error.getStatusCode(), error.getStatusText(), error.getResponseBodyAsString());
            }
            throw new RuntimeException(e);
        }
    }

    private long getIntervalInSeconds() {
        long interval = 1000;
        try {
            interval *= Long.parseLong(logWaitIntervalInSeconds);
        } catch (NumberFormatException e) {
            logger.warn("Wrong value for log.wait.interval [{}]. Default value for interval is 1 second.", logWaitIntervalInSeconds);
        }
        return interval;
    }

    private HttpHeaders createHeadersWithMediaType() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    public void downloadDataToFile(UrlBuilder urlBuilder, final String filePath) throws Exception {
        boolean downloadComplete = false;

        validateToken();

        String url = urlBuilder.build();

        int attempt = 1;
        do {
            logger.info("Attempt {}. REST Request sent to [{}].", attempt, url);

            try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
                HttpGet request = new HttpGet(url);
                request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);

                HttpResponse response = client.execute(request);

                int responseCode = response.getStatusLine().getStatusCode();

                logger.debug("Response Code: {}", responseCode);
                if (responseCode == HttpStatus.OK.value()) {
                    logger.debug("Saving response to file: [{}]", filePath);

                    org.apache.http.HttpEntity entity = response.getEntity();
                    if (logWhileWaitingForResponse) {
                        String subject = "download from";
                        logWhileWaiting(subject, url, (Callable<Void>) () -> {
                            FileUtils.copyInputStreamToFile(entity.getContent(), new File(filePath));
                            return null;
                        });
                    } else {
                        FileUtils.copyInputStreamToFile(entity.getContent(), new File(filePath));
                    }
                    logger.debug("File download completed.");
                    downloadComplete = true;
                } else {
                    logger.error("Error while downloading file from Palantir: [{}] {}", url, response.getStatusLine().getReasonPhrase());
                }
            } catch (IOException e) {
                logger.error("Error while downloading file from Palantir.", e);
            } finally {
                attempt++;
            }
        } while (attempt <= getRequestRetryLimitInt() && !downloadComplete);

        if (!downloadComplete) {
            String errMsg = String.format("[%d] attempts. Unable to download data from [%s].", attempt - 1, url);
            logger.error(errMsg);
            throw new Exception(errMsg);
        }
    }
}
