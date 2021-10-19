package com.google.cloud.teleport.newrelic.config;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * The {@link NewRelicPipelineOptions} class provides the custom options passed by the executor at the command line
 * to configure the pipeline that process PubSub data and sends it to NR using {@link com.google.cloud.teleport.newrelic.NewRelicEventWriter}.
 */
public interface NewRelicPipelineOptions extends PipelineOptions {
    @Description("NewRelic insert API key.")
    ValueProvider<String> getApiKey();

    void setApiKey(ValueProvider<String> apiKey);

    @Description("NewRelic log api url. This should be routable from the VPC in which the Dataflow pipeline runs.")
    ValueProvider<String> getUrl();

    void setUrl(ValueProvider<String> url);

    @Description("Batch count for sending multiple events to NewRelic in a single POST.")
    ValueProvider<Integer> getBatchCount();

    void setBatchCount(ValueProvider<Integer> batchCount);

    @Description("Disable SSL certificate validation.")
    ValueProvider<Boolean> getDisableCertificateValidation();

    void setDisableCertificateValidation(ValueProvider<Boolean> disableCertificateValidation);

    @Description("Maximum number of parallel requests.")
    ValueProvider<Integer> getParallelism();

    void setParallelism(ValueProvider<Integer> parallelism);

    @Description("KMS Encryption Key for the token. The Key should be in the format "
            + "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    ValueProvider<String> getTokenKMSEncryptionKey();

    void setTokenKMSEncryptionKey(ValueProvider<String> keyName);

    @Description("True to gzip payloads to log API.")
    ValueProvider<Boolean> getUseCompression();

    void setUseCompression(ValueProvider<Boolean> useCompression);
}