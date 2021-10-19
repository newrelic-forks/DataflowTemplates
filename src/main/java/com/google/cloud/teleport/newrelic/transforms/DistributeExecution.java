package com.google.cloud.teleport.newrelic.transforms;

import com.google.cloud.teleport.newrelic.NewRelicEvent;
import com.google.cloud.teleport.newrelic.NewRelicEventCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * This PTransform adds a Key to each processed NewRelicEvent, resulting in a key-value pair (where the value is the
 * NewRelicEvent). This will effectively parallelize the execution, since all the records having the same key will
 * be processed by the same worker instance.
 */
public class DistributeExecution extends
        PTransform<PCollection<NewRelicEvent>, PCollection<KV<Integer, NewRelicEvent>>> {

    private static final Logger LOG = LoggerFactory.getLogger(DistributeExecution.class);

    private static final Integer DEFAULT_PARALLELISM = 1;

    private ValueProvider<Integer> specifiedParallelism;

    private DistributeExecution(ValueProvider<Integer> specifiedParallelism) {
        this.specifiedParallelism = specifiedParallelism;
    }

    public static DistributeExecution withParallelism(ValueProvider<Integer> specifiedParallelism) {
        return new DistributeExecution(specifiedParallelism);
    }

    @Override
    public PCollection<KV<Integer, NewRelicEvent>> expand(PCollection<NewRelicEvent> input) {

        return input
                .apply("Inject Keys",
                        ParDo.of(new InjectKeysFn(this.specifiedParallelism))
                ).setCoder(KvCoder.of(BigEndianIntegerCoder.of(), NewRelicEventCoder.of()));

    }

    private class InjectKeysFn extends DoFn<NewRelicEvent, KV<Integer, NewRelicEvent>> {

        private ValueProvider<Integer> specifiedParallelism;
        private Integer calculatedParallelism;

        InjectKeysFn(ValueProvider<Integer> specifiedParallelism) {
            this.specifiedParallelism = specifiedParallelism;
        }

        @Setup
        public void setup() {

            if (calculatedParallelism == null) {

                if (specifiedParallelism != null) {
                    calculatedParallelism = specifiedParallelism.get();
                }

                calculatedParallelism =
                        MoreObjects.firstNonNull(calculatedParallelism, DEFAULT_PARALLELISM);

                LOG.info("Parallelism set to: {}", calculatedParallelism);
            }
        }

        @ProcessElement
        public void processElement(DoFn<NewRelicEvent, KV<Integer, NewRelicEvent>>.ProcessContext context) {
            context.output(
                    KV.of(ThreadLocalRandom.current().nextInt(calculatedParallelism), context.element()));
        }
    }
}