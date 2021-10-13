package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.newrelic.NewRelicConverter;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadSubscriptionOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

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
 * PIPELINE_BUCKET_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigquery
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToNewRelic \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_BUCKET_FOLDER}/staging \
 * --tempLocation=${PIPELINE_BUCKET_FOLDER}/temp \
 * --templateLocation=${PIPELINE_BUCKET_FOLDER}/template/PubSubToNewRelic \
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
        return null;
    }

    /**
     * The {@link PubSubToNewRelicOptions} class provides the custom options passed by the executor at
     * the command line.
     */
    public interface PubSubToNewRelicOptions
            extends NewRelicConverter.NewRelicOptions,
            PubsubReadSubscriptionOptions {}
}
