package com.google.cloud.teleport.newrelic.config;

import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider;

public class NewRelicConfig {
    private final ValueProvider<String> url;
    private final ValueProvider<String> apiKey;
    private final ValueProvider<Integer> batchCount;
    private final ValueProvider<Integer> parallelism;
    private final ValueProvider<Boolean> disableCertificateValidation;
    private final ValueProvider<Boolean> useCompression;

    private NewRelicConfig(final ValueProvider<String> url,
                           final ValueProvider<String> apiKey,
                           final ValueProvider<Integer> batchCount,
                           final ValueProvider<Integer> parallelism,
                           final ValueProvider<Boolean> disableCertificateValidation,
                           final ValueProvider<Boolean> useCompression) {
        this.url = url;
        this.apiKey = apiKey;
        this.batchCount = batchCount;
        this.parallelism = parallelism;
        this.disableCertificateValidation = disableCertificateValidation;
        this.useCompression = useCompression;
    }

    public static NewRelicConfig fromPipelineOptions(final NewRelicPipelineOptions newRelicOptions) {
        return new NewRelicConfig(
                newRelicOptions.getUrl(),
                newRelicOptions.getTokenKMSEncryptionKey().isAccessible()
                        ? maybeDecrypt(newRelicOptions.getApiKey(), newRelicOptions.getTokenKMSEncryptionKey())
                        : newRelicOptions.getApiKey(),
                newRelicOptions.getBatchCount(),
                newRelicOptions.getParallelism(),
                newRelicOptions.getDisableCertificateValidation(),
                newRelicOptions.getUseCompression());
    }

    /**
     * Utility method to decrypt a NewRelic API token.
     *
     * @param unencryptedToken The NewRelic API token as a Base64 encoded {@link String} encrypted with a Cloud KMS Key.
     * @param kmsKey           The Cloud KMS Encryption Key to decrypt the NewRelic API token.
     * @return Decrypted NewRelic API token.
     */
    private static ValueProvider<String> maybeDecrypt(
            ValueProvider<String> unencryptedToken, ValueProvider<String> kmsKey) {
        return new KMSEncryptedNestedValueProvider(unencryptedToken, kmsKey);
    }

    public ValueProvider<String> getUrl() {
        return url;
    }

    public ValueProvider<String> getApiKey() {
        return apiKey;
    }

    public ValueProvider<Integer> getBatchCount() {
        return batchCount;
    }

    public ValueProvider<Integer> getParallelism() {
        return parallelism;
    }

    public ValueProvider<Boolean> getDisableCertificateValidation() {
        return disableCertificateValidation;
    }

    public ValueProvider<Boolean> getUseCompression() {
        return useCompression;
    }
}
