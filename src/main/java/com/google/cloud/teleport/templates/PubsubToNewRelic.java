package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.newrelic.NewRelicPipeline;
import com.google.cloud.teleport.newrelic.config.NewRelicConfig;
import com.google.cloud.teleport.newrelic.config.PubSubToNewRelicPipelineOptions;
import com.google.cloud.teleport.newrelic.transforms.ReadMessagesFromPubSub;
import com.google.cloud.teleport.newrelic.transforms.NewRelicIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * The {@link PubsubToNewRelic} pipeline is a streaming pipeline which ingests data from Cloud
 * Pub/Sub, executes a UDF, converts the output to {@link NewRelicLogRecord}s and writes those records
 * into NewRelic's API endpoint. Any errors which occur in the execution of the UDF, conversion to
 * {@link NewRelicLogRecord} or writing to API will be streamed into a Pub/Sub topic.
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

    /**
     * String/String Coder for FailsafeElement.
     */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * PubsubToNewRelic#run(PubSubToNewRelicPipelineOptions)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {

        final PubSubToNewRelicPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToNewRelicPipelineOptions.class);

        run(options);
    }

    public static PipelineResult run(PubSubToNewRelicPipelineOptions options) {
        final Pipeline pipeline = Pipeline.create(options);

        final NewRelicPipeline nrPipeline = new NewRelicPipeline(
                pipeline,
                new ReadMessagesFromPubSub(options.getInputSubscription()),
                new NewRelicIO(NewRelicConfig.fromPipelineOptions(options))
        );

        return nrPipeline.run();
    }
}
