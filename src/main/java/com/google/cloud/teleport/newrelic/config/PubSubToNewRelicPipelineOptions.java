package com.google.cloud.teleport.newrelic.config;

import com.google.cloud.teleport.templates.common.PubsubConverters;

/**
 * The {@link PubSubToNewRelicPipelineOptions} class provides the custom options passed by the executor at
 * the command line to execute the {@link com.google.cloud.teleport.templates.PubsubToNewRelic} template.
 * It includes:
 *   - The options to read from a Pubsub subscription ({@link PubsubConverters.PubsubReadSubscriptionOptions}
 *   - The NewRelic-specific options to send logs to New Relic Logs ({@link NewRelicPipelineOptions}.
 */
public interface PubSubToNewRelicPipelineOptions extends NewRelicPipelineOptions, PubsubConverters.PubsubReadSubscriptionOptions {
}