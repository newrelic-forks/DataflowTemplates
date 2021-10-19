package com.google.cloud.teleport.newrelic.transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.nio.charset.StandardCharsets;

public class ReadMessagesFromPubSub extends PTransform<PBegin, PCollection<String>> {

    private final ValueProvider<String> subscriptionName;

    public ReadMessagesFromPubSub(ValueProvider<String> subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
        return input
                .apply(
                        "ReadPubsubMessage",
                        PubsubIO.readMessagesWithAttributes().fromSubscription(subscriptionName))
                .apply(
                        "ExtractDataAsString",
                        ParDo.of(
                                new DoFn<PubsubMessage, String>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        // TODO Check whether we should also consider the attribute map present in the PubsubMessage
                                        context.output(new String(context.element().getPayload(), StandardCharsets.UTF_8));
                                    }
                                }));
    }
}