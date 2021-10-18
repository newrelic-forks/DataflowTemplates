package com.google.cloud.teleport.newrelic;

import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

public class NewRelicPipeline {

    /** String/String Coder for FailsafeElement. */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    /** The tag for successful {@link NewRelicEvent} conversion. */
    private static final TupleTag<NewRelicEvent> NewRelic_EVENT_OUT = new TupleTag<NewRelicEvent>() {};

    /** The tag for failed {@link NewRelicEvent} conversion. */
    private static final TupleTag<FailsafeElement<String, String>> NewRelic_EVENT_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<String, String>>() {};

    private final PTransform<PBegin, PCollection<String>> pubsubMessageReaderTransform;
    private final PTransform<PCollection<NewRelicEvent>, PCollection<NewRelicWriteError>> newrelicMessageWriterTransform;
    private final Pipeline pipeline;

    public NewRelicPipeline(Pipeline pipeline,
                            PTransform<PBegin, PCollection<String>> pubsubMessageReaderTransform,
                            PTransform<PCollection<NewRelicEvent>, PCollection<NewRelicWriteError>> newrelicMessageWriterTransform) {
        this.pipeline = pipeline;
        this.pubsubMessageReaderTransform = pubsubMessageReaderTransform;
        this.newrelicMessageWriterTransform = newrelicMessageWriterTransform;
    }

    /*
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options: the execution options.
     * @return The pipeline result.
     */
    public PipelineResult run(){

        // Register New relic amd failsafe coders.
        CoderRegistry registry = pipeline.getCoderRegistry();
        registry.registerCoderForClass(NewRelicEvent.class, NewRelicEventCoder.of());
        registry.registerCoderForType(
                FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

        // 1) Read messages in from Pub/Sub
        PCollection<String> stringMessages =
                pipeline.apply("Read messages from subscription",
                        pubsubMessageReaderTransform);

        // 2) Convert message to FailsafeElement for processing.
        PCollection<FailsafeElement<String, String>> transformedOutput =
                stringMessages
                        .apply(
                                "Transform to Failsafe Element",
                                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                                        .via(input -> FailsafeElement.of(input, input)));

        // 3) Convert successfully transformed messages into NewRelicEvent objects
        PCollectionTuple convertToEventTuple = transformedOutput
                .apply("Transform to New Relic Event", NewRelicConverters.failsafeStringToNewRelicEvent(
                        NewRelic_EVENT_OUT, NewRelic_EVENT_DEADLETTER_OUT));

        // 4) Write NewRelicEvents to NewRelic's Log API end point.
        convertToEventTuple
                .get(NewRelic_EVENT_OUT)
                .apply("Forward event to New Relic", newrelicMessageWriterTransform);

        return pipeline.run();
    }
}
