package com.google.cloud.teleport.newrelic;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * {@link PTransform}s {@link com.google.pubsub.v1.PubsubMessage} to {@link NewRelicEvent}
 * Available {@link PipelineOptions} used by the pipeline that process sink data and send to NR using {@link NewRelicIO}.
 */
public class NewRelicConverter {


    /**
     * The {@link NewRelicOptions} class provides the custom options passed by the
     * executor at the command line.
     */
    public interface NewRelicOptions extends PipelineOptions {

    }
}
