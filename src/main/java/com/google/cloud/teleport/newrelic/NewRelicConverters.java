package com.google.cloud.teleport.newrelic;

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s {@link com.google.pubsub.v1.PubsubMessage} to {@link NewRelicEvent}
 * Available {@link PipelineOptions} used by the pipeline that process sink data and send to NR using {@link NewRelicIO}.
 */
public class NewRelicConverters {

    private static final Logger LOG = LoggerFactory.getLogger(NewRelicConverters.class);

    /**
     * Returns a {@link FailsafeStringToNewRelicEvent} {@link PTransform} that
     * consumes {@link FailsafeElement} messages and creates {@link NewRelicEvent}
     * objects. Any conversion errors are wrapped into a {@link FailsafeElement}
     * with appropriate error information.
     *
     * @param nrEventOutputTag {@link TupleTag} to use for successfully converted
     *                         messages.
     * @param nrDeadletterTag  {@link TupleTag} to use for messages that failed
     *                         conversion.
     */
    public static FailsafeStringToNewRelicEvent failsafeStringToNewRelicEvent(TupleTag<NewRelicEvent> nrEventOutputTag,
                                                                              TupleTag<FailsafeElement<String, String>> nrDeadletterTag) {
        return new FailsafeStringToNewRelicEvent(nrEventOutputTag, nrDeadletterTag);
    }

    /**
     * The {@link NewRelicOptions} class provides the custom options passed by the
     * executor at the command line.
     */
    public interface NewRelicOptions extends PipelineOptions {
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

    private static class FailsafeStringToNewRelicEvent
            extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

        private static final String TIMESTAMP_KEY = "timestamp";

        private static final Counter CONVERSION_ERRORS = Metrics.counter(FailsafeStringToNewRelicEvent.class,
                "newrelic-event-conversion-errors");

        private static final Counter CONVERSION_SUCCESS = Metrics.counter(FailsafeStringToNewRelicEvent.class,
                "newrelic-event-conversion-successes");

        private TupleTag<NewRelicEvent> eventOutputTag;
        private TupleTag<FailsafeElement<String, String>> deadletterTag;

        FailsafeStringToNewRelicEvent(TupleTag<NewRelicEvent> eventOutputTag,
                                      TupleTag<FailsafeElement<String, String>> deadletterTag) {
            this.eventOutputTag = eventOutputTag;
            this.deadletterTag = deadletterTag;
        }

        @Override
        public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {

            return input.apply("ConvertToNewRelicEvent", ParDo.of(new DoFn<FailsafeElement<String, String>, NewRelicEvent>() {

                @ProcessElement
                public void processElement(ProcessContext context) {

                    String input = context.element().getPayload();

                    try {
                        // Start building a NewRelicEvent with the payload as the message.
                        NewRelicEvent nrEvent = new NewRelicEvent();
                        nrEvent.setMessage(input);

                        // We will attempt to parse the input to see
                        // if it is a valid JSON and if so, whether we can
                        // extract some additional properties that would be
                        // present in Stackdriver's LogEntry structure (timestamp) or
                        // a user provided _metadata field.
                        try {

                            JSONObject json = new JSONObject(input);

                            String parsedTimestamp = json.optString(TIMESTAMP_KEY);

                            if (!parsedTimestamp.isEmpty()) {
                                try {
                                    nrEvent.setTimestamp(DateTime.parseRfc3339(parsedTimestamp).getValue());
                                } catch (NumberFormatException n) {
                                    // We log this exception but don't want to fail the entire record.
                                    LOG.debug("Unable to parse non-rfc3339 formatted timestamp: {}", parsedTimestamp);
                                }
                            }

                        } catch (JSONException je) {
                            // input is either not a properly formatted JSONObject
                            // or has other exceptions. In this case, we will
                            // simply capture the entire input as an 'event' and
                            // not worry about capturing any specific properties
                            // (for e.g Timestamp etc).
                            // We also do not want to LOG this as we might be running
                            // a pipeline to simply log text entries to NewRelic and
                            // this is expected behavior.
                        }

                        context.output(eventOutputTag, nrEvent);
                        CONVERSION_SUCCESS.inc();

                    } catch (Exception e) {
                        CONVERSION_ERRORS.inc();
                        context.output(deadletterTag, FailsafeElement.of(input, input).setErrorMessage(e.getMessage())
                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                    }
                }
            }).withOutputTags(eventOutputTag, TupleTagList.of(deadletterTag)));
        }
    }
}
