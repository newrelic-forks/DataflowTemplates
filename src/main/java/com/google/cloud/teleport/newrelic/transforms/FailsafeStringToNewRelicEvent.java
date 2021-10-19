package com.google.cloud.teleport.newrelic.transforms;

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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
 * {@link PTransform}s messages (either as plain strings or as JSON strings) to {@link NewRelicLogRecord}s
 */
public class FailsafeStringToNewRelicEvent
        extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(FailsafeStringToNewRelicEvent.class);

    private static final String TIMESTAMP_KEY = "timestamp";

    private static final Counter CONVERSION_ERRORS = Metrics.counter(FailsafeStringToNewRelicEvent.class,
            "newrelic-event-conversion-errors");

    private static final Counter CONVERSION_SUCCESS = Metrics.counter(FailsafeStringToNewRelicEvent.class,
            "newrelic-event-conversion-successes");

    private TupleTag<NewRelicLogRecord> eventOutputTag;
    private TupleTag<FailsafeElement<String, String>> deadletterTag;

    private FailsafeStringToNewRelicEvent(TupleTag<NewRelicLogRecord> eventOutputTag,
                                          TupleTag<FailsafeElement<String, String>> deadletterTag) {
        this.eventOutputTag = eventOutputTag;
        this.deadletterTag = deadletterTag;
    }

    /**
     * Returns a {@link FailsafeStringToNewRelicEvent} {@link PTransform} that
     * consumes {@link FailsafeElement} messages and creates {@link NewRelicLogRecord}
     * objects. Any conversion errors are wrapped into a {@link FailsafeElement}
     * with appropriate error information.
     *
     * @param nrEventOutputTag {@link TupleTag} to use for successfully converted
     *                         messages.
     * @param nrDeadletterTag  {@link TupleTag} to use for messages that failed
     *                         conversion.
     */
    public static FailsafeStringToNewRelicEvent withOutputTags(TupleTag<NewRelicLogRecord> nrEventOutputTag,
                                                               TupleTag<FailsafeElement<String, String>> nrDeadletterTag) {
        return new FailsafeStringToNewRelicEvent(nrEventOutputTag, nrDeadletterTag);
    }

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {

        return input.apply("ConvertToNewRelicEvent", ParDo.of(new DoFn<FailsafeElement<String, String>, NewRelicLogRecord>() {

            @ProcessElement
            public void processElement(ProcessContext context) {

                String input = context.element().getPayload();

                try {
                    // Start building a NewRelicEvent with the payload as the message.
                    NewRelicLogRecord nrEvent = new NewRelicLogRecord();
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
