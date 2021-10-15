package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.newrelic.NewRelicConverters;
import com.google.cloud.teleport.newrelic.NewRelicEvent;
import com.google.cloud.teleport.newrelic.NewRelicEventCoder;
import com.google.cloud.teleport.newrelic.NewRelicIO;
import com.google.cloud.teleport.newrelic.NewRelicPipeline;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadSubscriptionOptions;
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import com.google.cloud.teleport.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * The {@link PubsubToNewRelic} pipeline is a streaming pipeline which ingests data from Cloud
 * Pub/Sub, executes a UDF, converts the output to {@link NewRelicEvent}s and writes those records
 * into NewRelic's API endpoint. Any errors which occur in the execution of the UDF, conversion to
 * {@link NewRelicEvent} or writing to API will be streamed into a Pub/Sub topic.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The source Pub/Sub subscription exists.
 *   <li>API end-point is routable from the VPC where the Dataflow job executes.
 *   <li>Deadletter topic exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_BUCKET_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-newrelic
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.PubsubToNewRelic \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_BUCKET_FOLDER}/staging \
 * --tempLocation=${PIPELINE_BUCKET_FOLDER}/temp \
 * --templateLocation=${PIPELINE_BUCKET_FOLDER}/template/PubsubToNewRelic \
 * --runner=${RUNNER}
 * "
 *
 * # Execute the template
 * JOB_NAME=pubsub-to-NewRelic-$USER-`date +"%Y%m%d-%H%M%S%z"`
 * BATCH_COUNT=1
 * PARALLELISM=5
 * REGION=us-west1
 *
 * INPUT_SUB_NAME=SUB NAME WHERE LOGS ARE
 * DEADLETTER_TOPIC_NAME=TOPIC TO FORWARDING UNDELIVERED MESSAGES
 *
 * NR_LOG_ENDPOINT=https://log-api.newrelic.com/log/v1
 *
 * NR_API_KEY=YOUR NEW RELIC API KEY
 *
 *
 * # Execute the templated pipeline:
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template/PubSubToNewRelic \
 * --region=${REGION} \
 * --parameters \
 * "inputSubscription=projects/${PROJECT_ID}/subscriptions/${INPUT_SUB_NAME},\
 * apiKey=${NR_API_KEY},\
 * url=${NR_LOG_ENDPOINT},\
 * batchCount=${BATCH_COUNT},\
 * parallelism=${PARALLELISM},\
 * disableCertificateValidation=false,\
 * outputDeadletterTopic=projects/${PROJECT_ID}/topics/${DEADLETTER_TOPIC_NAME}"
 * </pre>
 */
public class PubsubToNewRelic {

    /** String/String Coder for FailsafeElement. */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    /** The tag for successful {@link NewRelicEvent} conversion. */
    private static final TupleTag<NewRelicEvent> NewRelic_EVENT_OUT = new TupleTag<NewRelicEvent>() {};

    /** The tag for failed {@link NewRelicEvent} conversion. */
    private static final TupleTag<FailsafeElement<String, String>> NewRelic_EVENT_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<String, String>>() {};

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * PubsubToNewRelic#run(PubSubToNewRelicOptions)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {

        PubSubToNewRelicOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToNewRelicOptions.class);

        run(options);
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
    public static PipelineResult run(PubSubToNewRelicOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        // Register New relic amd failsafe coders.
        CoderRegistry registry = pipeline.getCoderRegistry();
        registry.registerCoderForClass(NewRelicEvent.class, NewRelicEventCoder.of());
        registry.registerCoderForType(
                FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

        /*
         * Pipeline steps:
         *  1) Read messages in from Pub/Sub
         *  2) Convert message to FailsafeElement for processing.
         *  3) Convert successfully transformed messages into NewRelicEvent objects
         *  4) Write NewRelicEvents to NewRelic's Log API endpoint.
         */

        // 1) Read messages in from Pub/Sub
        PCollection<String> stringMessages =
                pipeline.apply(
                        "Read messages from subscription",
                        new ReadMessages(options.getInputSubscription()));

        // 2) Convert message to FailsafeElement for processing.
        PCollection<FailsafeElement<String, String>> transformedOutput =
                stringMessages
                        .apply(
                                "Transform to Failsafe Element",
                                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                                        .via(input -> FailsafeElement.of(input, input)));

        // 3) Convert successfully transformed messages into NewRelicEvent objects
        PCollectionTuple convertToEventTuple =
                transformedOutput
                        .apply(
                                "Transform to New Relic Event",
                                NewRelicConverters.failsafeStringToNewRelicEvent(
                                        NewRelic_EVENT_OUT, NewRelic_EVENT_DEADLETTER_OUT));

        // 4) Write NewRelicEvents to NewRelic's Log API end point.
        convertToEventTuple
                .get(NewRelic_EVENT_OUT)
                .apply(
                        "Forward event to New Relic",
                        new NewRelicIO.Write(options.getUrl(),
                                options.getTokenKMSEncryptionKey().isAccessible() ? maybeDecrypt(options.getApiKey(),
                                        options.getTokenKMSEncryptionKey()) : options.getApiKey(),
                                options.getBatchCount(), options.getParallelism(),
                                options.getDisableCertificateValidation(), options.getUseCompression()));

        return pipeline.run();
    }

    /**
     * Utility method to decrypt a NewRelic API token.
     *
     * @param unencryptedToken The NewRelic API token as a Base64 encoded {@link String} encrypted with a Cloud KMS Key.
     * @param kmsKey The Cloud KMS Encryption Key to decrypt the NewRelic API token.
     * @return Decrypted NewRelic API token.
     */
    private static ValueProvider<String> maybeDecrypt(
            ValueProvider<String> unencryptedToken, ValueProvider<String> kmsKey) {
        return new KMSEncryptedNestedValueProvider(unencryptedToken, kmsKey);
    }

    /**
     * The {@link PubSubToNewRelicOptions} class provides the custom options passed by the executor at
     * the command line.
     */
    public interface PubSubToNewRelicOptions
            extends NewRelicConverters.NewRelicOptions,
    /**
     * A {@link PTransform} that reads messages from a Pub/Sub subscription, increments a counter and
     * returns a {@link PCollection} of {@link String} messages.
     */


            PubsubReadSubscriptionOptions {}

    public static class ReadMessages extends PTransform<PBegin, PCollection<String>> {

        private final ValueProvider<String> subscriptionName;

        ReadMessages(ValueProvider<String> subscriptionName) {
            this.subscriptionName = subscriptionName;
        }

        @Override
        public PCollection<String> expand(PBegin input) {
            return input
                    .apply(
                            "ReadPubsubMessage",
                            PubsubIO.readMessagesWithAttributes().fromSubscription(subscriptionName))
                    .apply(
                            "ExtractMessage",
                            ParDo.of(
                                    new DoFn<PubsubMessage, String>() {
                                        @ProcessElement
                                        public void processElement(ProcessContext context) {
                                            context.output(new String(context.element().getPayload(), StandardCharsets.UTF_8));
                                        }
                                    }));
        }
    }
}
