package com.google.cloud.teleport.newrelic.transforms;

import com.google.cloud.teleport.newrelic.NewRelicEvent;
import com.google.cloud.teleport.newrelic.NewRelicEventWriter;
import com.google.cloud.teleport.newrelic.NewRelicWriteError;
import com.google.cloud.teleport.newrelic.NewRelicWriteErrorCoder;
import com.google.cloud.teleport.newrelic.config.NewRelicConfig;
import com.google.cloud.teleport.newrelic.config.NewRelicPipelineOptions;
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Class {@link SendToNewRelic} provides a {@link PTransform} that allows writing {@link NewRelicEvent}
 * records into a NewRelic logs API end-point using HTTP POST requests. In the event of
 * an error, a {@link PCollection} of {@link NewRelicWriteError} records are returned for further
 * processing or storing into a deadletter sink.
 */
public class SendToNewRelic extends PTransform<PCollection<NewRelicEvent>, PCollection<NewRelicWriteError>> {

    private static final Logger LOG = LoggerFactory.getLogger(SendToNewRelic.class);

    private ValueProvider<String> url;
    private ValueProvider<String> apiKey;
    private ValueProvider<Integer> batchCount;
    private ValueProvider<Integer> parallelism;
    private ValueProvider<Boolean> disableCertificateValidation;
    private ValueProvider<Boolean> useCompression;

    public SendToNewRelic(NewRelicConfig newRelicConfig) {
        this.url = newRelicConfig.getUrl();
        this.apiKey = newRelicConfig.getApiKey();
        this.batchCount = newRelicConfig.getBatchCount();
        this.parallelism = newRelicConfig.getParallelism();
        this.disableCertificateValidation = newRelicConfig.getDisableCertificateValidation();
        this.useCompression = newRelicConfig.getUseCompression();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SendToNewRelic write = (SendToNewRelic) o;
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
                .apply("Distribute execution", DistributeExecution.withParallelism(parallelism))
                .apply("Write NewRelic events", ParDo.of(writer)).setCoder(NewRelicWriteErrorCoder.of());
    }
}
