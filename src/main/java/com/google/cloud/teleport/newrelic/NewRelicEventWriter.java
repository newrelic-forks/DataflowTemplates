/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.teleport.newrelic;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.teleport.newrelic.config.NewRelicConfig;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogApiSendError;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link DoFn} to write {@link NewRelicLogRecord}s to NewRelic's log API endpoint.
 */
public class NewRelicEventWriter extends DoFn<KV<Integer, NewRelicLogRecord>, NewRelicLogApiSendError> {

    private static final Logger LOG = LoggerFactory.getLogger(NewRelicEventWriter.class);
    private static final int DEFAULT_BATCH_COUNT = 1;
    private static final boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
    private static final boolean DEFAULT_USE_COMPRESSION = true;
    private static final long DEFAULT_FLUSH_DELAY = 2;
    private static final Counter INPUT_COUNTER = Metrics
            .counter(NewRelicEventWriter.class, "inbound-events");
    private static final Counter SUCCESS_WRITES = Metrics
            .counter(NewRelicEventWriter.class, "outbound-successful-events");
    private static final Counter FAILED_WRITES = Metrics
            .counter(NewRelicEventWriter.class, "outbound-failed-events");
    private static final String BUFFER_STATE_NAME = "buffer";
    private static final String COUNT_STATE_NAME = "count";
    private static final String TIME_ID_NAME = "expiry";
    private static final Gson GSON =
            new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

    @StateId(BUFFER_STATE_NAME)
    private final StateSpec<BagState<NewRelicLogRecord>> buffer = StateSpecs.bag();
    @StateId(COUNT_STATE_NAME)
    private final StateSpec<ValueState<Long>> count = StateSpecs.value();
    @TimerId(TIME_ID_NAME)
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    // Non-serialized fields: these are set up once the DoFn has potentially been deserialized, in the @Setup method.
    private Integer batchCount;
    private Boolean disableCertificateValidation;
    private Boolean useCompression;
    private HttpClient publisher;

    // Serialized fields
    private ValueProvider<String> url;
    private ValueProvider<String> apiKey;
    private ValueProvider<Boolean> inputDisableCertificateValidation;
    private ValueProvider<Integer> inputBatchCount;
    private ValueProvider<Boolean> inputUseCompression;

    public NewRelicEventWriter(final NewRelicConfig newRelicConfig) {
        this.url = newRelicConfig.getUrl();
        this.apiKey = newRelicConfig.getApiKey();
        this.inputDisableCertificateValidation = newRelicConfig.getDisableCertificateValidation();
        this.inputBatchCount = newRelicConfig.getBatchCount();
        this.inputUseCompression = newRelicConfig.getUseCompression();
    }

    @Setup
    public void setup() {

        checkArgument(url != null && url.isAccessible(), "url is required for writing events.");
        checkArgument(apiKey != null && apiKey.isAccessible(), "API key is required for writing events.");

        batchCount = inputBatchCount != null && inputBatchCount.isAccessible()
                ? inputBatchCount.get()
                : DEFAULT_BATCH_COUNT;
        LOG.info("Batch count set to: {}", batchCount);

        disableCertificateValidation = inputDisableCertificateValidation != null && inputDisableCertificateValidation.isAccessible()
                ? inputDisableCertificateValidation.get()
                : DEFAULT_DISABLE_CERTIFICATE_VALIDATION;
        LOG.info("Disable certificate validation set to: {}", disableCertificateValidation);

        useCompression = inputUseCompression != null && inputUseCompression.isAccessible()
                ? inputUseCompression.get()
                : DEFAULT_USE_COMPRESSION;
        LOG.info("Use Compression set to: {}", useCompression);

        try {
            this.publisher = new HttpClient();
            publisher.setGenericUrl(new GenericUrl(url.get()));
            publisher.setApiKey(apiKey.get());
            publisher.setDisableCertificateValidation(disableCertificateValidation);
            publisher.setUseCompression(useCompression);
            publisher.init();

            LOG.info("Successfully created HttpEventPublisher");

        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            LOG.error("Error creating HttpEventPublisher: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @ProcessElement
    public void processElement(
            @Element KV<Integer, NewRelicLogRecord> input,
            OutputReceiver<NewRelicLogApiSendError> receiver,
            @StateId(BUFFER_STATE_NAME) BagState<NewRelicLogRecord> bufferState,
            @StateId(COUNT_STATE_NAME) ValueState<Long> countState,
            @TimerId(TIME_ID_NAME) Timer timer) throws IOException {

        Long count = MoreObjects.<Long>firstNonNull(countState.read(), 0L);
        NewRelicLogRecord event = input.getValue();
        INPUT_COUNTER.inc();
        bufferState.add(event);
        count += 1;
        countState.write(count);
        timer.offset(Duration.standardSeconds(DEFAULT_FLUSH_DELAY)).setRelative();

        if (count >= batchCount) {

            // LOG.info("Flushing batch of {} events", count);
            flush(receiver, bufferState, countState);
        }
    }

    @OnTimer(TIME_ID_NAME)
    public void onExpiry(OutputReceiver<NewRelicLogApiSendError> receiver,
                         @StateId(BUFFER_STATE_NAME) BagState<NewRelicLogRecord> bufferState,
                         @StateId(COUNT_STATE_NAME) ValueState<Long> countState) throws IOException {

        if (MoreObjects.<Long>firstNonNull(countState.read(), 0L) > 0) {
            // LOG.info("Flushing window with {} events", countState.read());
            flush(receiver, bufferState, countState);
        }
    }

    @Teardown
    public void tearDown() {
        if (this.publisher != null) {
            try {
                this.publisher.close();
                LOG.info("Successfully closed HttpEventPublisher");

            } catch (IOException e) {
                LOG.warn("Received exception while closing HttpEventPublisher: {}", e.getMessage());
            }
        }
    }

    /**
     * Utility method to flush a batch of requests via {@link HttpClient}.
     *
     * @param receiver Receiver to write {@link NewRelicLogApiSendError}s to
     */
    private void flush(
            OutputReceiver<NewRelicLogApiSendError> receiver,
            @StateId(BUFFER_STATE_NAME) BagState<NewRelicLogRecord> bufferState,
            @StateId(COUNT_STATE_NAME) ValueState<Long> countState) throws IOException {

        if (!bufferState.isEmpty().read()) {

            HttpResponse response = null;
            List<NewRelicLogRecord> events = Lists.newArrayList(bufferState.read());
            try {
                // Important to close this response to avoid connection leak.
                long startTime = System.currentTimeMillis();
                response = publisher.execute(events);
                long duration = System.currentTimeMillis() - startTime;

                if (!response.isSuccessStatusCode()) {
                    flushWriteFailures(
                            events, response.getStatusMessage(), response.getStatusCode(), receiver);
                    logWriteFailures(countState);

                } else {
                    StringBuilder textBuilder = new StringBuilder();
                    try (Reader reader = new BufferedReader(new InputStreamReader
                            (response.getContent(), Charset.forName(StandardCharsets.UTF_8.name())))) {
                        int c = 0;
                        while ((c = reader.read()) != -1) {
                            textBuilder.append((char) c);
                        }
                    }

                    LOG.info("Successfully wrote {} events in {}ms. Response code {} and body: {}",
                            countState.read(), duration, response.getStatusCode(), textBuilder.toString());
                    SUCCESS_WRITES.inc(countState.read());
                }

            } catch (HttpResponseException e) {
                LOG.error(
                        "Error writing to NewRelic. StatusCode: {}, content: {}, StatusMessage: {}",
                        e.getStatusCode(), e.getContent(), e.getStatusMessage());
                logWriteFailures(countState);

                flushWriteFailures(events, e.getStatusMessage(), e.getStatusCode(), receiver);

            } catch (IOException ioe) {
                LOG.error("Error writing to NewRelic: {}", ioe.getMessage());
                logWriteFailures(countState);

                flushWriteFailures(events, ioe.getMessage(), null, receiver);

            } finally {
                // States are cleared regardless of write success or failure since we
                // write failed events to an output PCollection.
                bufferState.clear();
                countState.clear();

                if (response != null) {
                    response.disconnect();
                }
            }
        }
    }

    /**
     * Utility method to un-batch and flush failed write events.
     *
     * @param events List of {@link NewRelicLogRecord}s to un-batch
     * @param statusMessage Status message to be added to {@link NewRelicLogApiSendError}
     * @param statusCode Status code to be added to {@link NewRelicLogApiSendError}
     * @param receiver Receiver to write {@link NewRelicLogApiSendError}s to
     */
    private static void flushWriteFailures(
            List<NewRelicLogRecord> events,
            String statusMessage,
            Integer statusCode,
            OutputReceiver<NewRelicLogApiSendError> receiver) {

        checkNotNull(events, "NewRelicEvents cannot be null.");

        for (NewRelicLogRecord event : events) {
            String payload = GSON.toJson(event);

            NewRelicLogApiSendError error = new NewRelicLogApiSendError();
            error.setStatusMessage(statusMessage);
            error.setStatusCode(statusCode);
            error.setPayload(payload);

            receiver.output(error);
        }
    }

    /**
     * Utility method to log write failures and handle metrics.
     */
    private void logWriteFailures(@StateId(COUNT_STATE_NAME) ValueState<Long> countState) {
        LOG.error("Failed to write {} events", countState.read());
        FAILED_WRITES.inc(countState.read());
    }
}
