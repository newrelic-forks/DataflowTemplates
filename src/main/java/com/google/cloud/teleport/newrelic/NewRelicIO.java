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

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link NewRelicIO} class provides a {@link PTransform} that allows writing {@link NewRelicEvent}
 * messages into a NewRelic Logs API end point.
 */
@Experimental(Kind.SOURCE_SINK)
public class NewRelicIO {

    private static final Logger LOG = LoggerFactory.getLogger(NewRelicIO.class);

    /**
     * Class {@link Write} provides a {@link PTransform} that allows writing {@link NewRelicEvent}
     * records into a NewRelic logs API end-point using HTTP POST requests. In the event of
     * an error, a {@link PCollection} of {@link NewRelicWriteError} records are returned for further
     * processing or storing into a deadletter sink.
     */
    // TODO - Extract this class since NewRelicIO serves no purpose above it.
    // TODO - Remove setters?
    public static class Write extends PTransform<PCollection<NewRelicEvent>, PCollection<NewRelicWriteError>> {

        private ValueProvider<String> url;
        private ValueProvider<String> apiKey;
        private ValueProvider<Integer> batchCount;
        private ValueProvider<Integer> parallelism;
        private ValueProvider<Boolean> disableCertificateValidation;
        private ValueProvider<Boolean> useCompression;

        public Write(ValueProvider<String> url, ValueProvider<String> apiKey, ValueProvider<Integer> batchCount,
                     ValueProvider<Integer> parallelism, ValueProvider<Boolean> disableCertificateValidation,
                     ValueProvider<Boolean> useCompression) {
            this.url = url;
            this.apiKey = apiKey;
            this.batchCount = batchCount;
            this.parallelism = parallelism;
            this.disableCertificateValidation = disableCertificateValidation;
            this.useCompression = useCompression;
        }

        public ValueProvider<String> getUrl() {
            return url;
        }

        /**
         * Method to set the url for the NewRelic Log API.
         * @param url for NewRelic Log API
         */
        public void setUrl(ValueProvider<String> url) {
            this.url = url;
        }

        public ValueProvider<String> getApiKey() {
            return apiKey;
        }

        /**
         * Method to set the API key for the log API.
         * @param apiKey for the log API.
         */
        public void setApiKey(ValueProvider<String> apiKey) {
            this.apiKey = apiKey;
        }

        public ValueProvider<Integer> getBatchCount() {
            return batchCount;
        }

        /**
         * Method to set the Batch Count.
         * @param batchCount for batching post requests.
         */
        public void setBatchCount(ValueProvider<Integer> batchCount) {
            this.batchCount = batchCount;
        }

        public ValueProvider<Integer> getParallelism() {
            return parallelism;
        }

        /**
         * Method to set the parallelism.
         * @param parallelism for controlling the number of http client connections.
         */
        public void setParallelism(ValueProvider<Integer> parallelism) {
            this.parallelism = parallelism;
        }

        public ValueProvider<Boolean> getDisableCertificateValidation() {
            return disableCertificateValidation;
        }

        /**
         * Method to disable certificate validation.
         * @param disableCertificateValidation for disabling certificate validation.
         */
        public void setDisableCertificateValidation(ValueProvider<Boolean> disableCertificateValidation) {
            this.disableCertificateValidation = disableCertificateValidation;
        }

        public ValueProvider<Boolean> getUseCompression() {
            return useCompression;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Write write = (Write) o;
            return url.equals(write.url) &&
                    apiKey.equals(write.apiKey) &&
                    batchCount.equals(write.batchCount) &&
                    parallelism.equals(write.parallelism) &&
                    disableCertificateValidation.equals(write.disableCertificateValidation) &&
                    useCompression.equals(write.useCompression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(url, apiKey, batchCount, parallelism, disableCertificateValidation, useCompression);
        }

        @Override
        public PCollection<NewRelicWriteError> expand(PCollection<NewRelicEvent> input) {

            LOG.info("Configuring NewRelicEventWriter.");
            NewRelicEventWriter writer = new NewRelicEventWriter();
            writer.setUrl(url);
            writer.setInputBatchCount(batchCount);
            writer.setDisableCertificateValidation(disableCertificateValidation);
            writer.setApiKey(apiKey);
            writer.setUseCompression(useCompression);

            LOG.info("NewRelicEventWriter configured");

            // Return a PCollection<NewRelicWriteError>
            return input
                    .apply("Create KV pairs", CreateKeys.of(parallelism))
                    .apply("Write NewRelic events", ParDo.of(writer)).setCoder(NewRelicWriteErrorCoder.of());
        }

        private static class CreateKeys extends
                PTransform<PCollection<NewRelicEvent>, PCollection<KV<Integer, NewRelicEvent>>> {

            private static final Integer DEFAULT_PARALLELISM = 1;

            private ValueProvider<Integer> requestedKeys;

            private CreateKeys(ValueProvider<Integer> requestedKeys) {
                this.requestedKeys = requestedKeys;
            }

            static CreateKeys of(ValueProvider<Integer> requestedKeys) {
                return new CreateKeys(requestedKeys);
            }

            @Override
            public PCollection<KV<Integer, NewRelicEvent>> expand(PCollection<NewRelicEvent> input) {

                return input
                        .apply("Inject Keys",
                                ParDo.of(new CreateKeysFn(this.requestedKeys))
                        ).setCoder(KvCoder.of(BigEndianIntegerCoder.of(), NewRelicEventCoder.of()));

            }

            private class CreateKeysFn extends DoFn<NewRelicEvent, KV<Integer, NewRelicEvent>> {

                private ValueProvider<Integer> specifiedParallelism;
                private Integer calculatedParallelism;

                CreateKeysFn(ValueProvider<Integer> specifiedParallelism) {
                    this.specifiedParallelism = specifiedParallelism;
                }

                @Setup
                public void setup() {

                    if (calculatedParallelism == null) {

                        if (specifiedParallelism != null) {
                            calculatedParallelism = specifiedParallelism.get();
                        }

                        calculatedParallelism =
                                MoreObjects.firstNonNull(calculatedParallelism, DEFAULT_PARALLELISM);

                        LOG.info("Parallelism set to: {}", calculatedParallelism);
                    }
                }

                @ProcessElement
                public void processElement(DoFn<NewRelicEvent, KV<Integer, NewRelicEvent>>.ProcessContext context) {
                    context.output(
                            KV.of(ThreadLocalRandom.current().nextInt(calculatedParallelism), context.element()));
                }

            }
        }
    }
}
