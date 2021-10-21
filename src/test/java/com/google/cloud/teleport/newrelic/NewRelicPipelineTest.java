package com.google.cloud.teleport.newrelic;

import com.google.cloud.teleport.newrelic.config.NewRelicConfig;
import com.google.cloud.teleport.newrelic.transforms.NewRelicIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.verify.VerificationTimes;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;

import static org.junit.Assume.assumeNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

/**
 * Unit tests for {@link NewRelicPipelineTest}.
 */
public class NewRelicPipelineTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();
    private final String EXPECTED_PATH = "/log/v1";
    private final String API_KEY = "an-api-key";

    private MockServerClient mockServerClient;
    private String url;
    @Before
    public void setUp() throws Exception {
        try {
            mockServerClient = startClientAndServer();
            url = String.format("http://localhost:%d%s", mockServerClient.getPort(), EXPECTED_PATH);
        } catch (Exception e) {
            assumeNoException(e);
        }
    }

    @After
    public void tearDown() {
        mockServerClient.stop();
    }

    @Test
    public void testPubSubMessagesAreSentToNewRelic() {
        mockServerClient
                .when(HttpRequest.request(EXPECTED_PATH))
                .respond(HttpResponse.response().withStatusCode(202));

        final String message = "A log message";
        final LocalDateTime messageTimestamp = LocalDateTime.of(2021, Month.DECEMBER, 25, 23, 0, 0, 900);
        final String jsonMessage = "{ \"message\": \"A JSON message\", \"timestamp\": \"" + messageTimestamp.toString() + "\"}";

        NewRelicPipeline pipeline = new NewRelicPipeline(
                testPipeline,
                Create.of(message, jsonMessage),
                new NewRelicIO(getNewRelicConfig(url, 10, 1, false)));

        pipeline.run().waitUntilFinish(Duration.millis(100));

        final String expectedBody = String.format("[ {\n" +
                "      \"message\" : \"" + message + "\"\n" +
                "    }, {\n" +
                "      \"message\" : \"" + StringEscapeUtils.escapeJava(jsonMessage) + "\",\n"
                + "\"timestamp\" : " + messageTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli() + "\n" +
                "    } ]");

        mockServerClient.verify(
                getRequestDefinition()
                        .withBody(JsonBody.json(expectedBody)),
                VerificationTimes.once());
    }

    private NewRelicConfig getNewRelicConfig(final String url, final Integer batchCount, final Integer parallelism, final Boolean useCompression) {
        final NewRelicConfig newRelicConfig = mock(NewRelicConfig.class);
        when(newRelicConfig.getUrl()).thenReturn(ValueProvider.StaticValueProvider.of(url));
        when(newRelicConfig.getApiKey()).thenReturn(ValueProvider.StaticValueProvider.of(API_KEY));
        when(newRelicConfig.getBatchCount()).thenReturn(ValueProvider.StaticValueProvider.of(batchCount));
        when(newRelicConfig.getParallelism()).thenReturn(ValueProvider.StaticValueProvider.of(parallelism));
        when(newRelicConfig.getDisableCertificateValidation()).thenReturn(ValueProvider.StaticValueProvider.of(false));
        when(newRelicConfig.getUseCompression()).thenReturn(ValueProvider.StaticValueProvider.of(useCompression));

        return newRelicConfig;
    }

    @Test
    public void testPubSubMessagesAreSentToNewRelicUsingDefaultsValues() {
        mockServerClient
                .when(HttpRequest.request(EXPECTED_PATH))
                .respond(HttpResponse.response().withStatusCode(202));

        final String message = "A log message";
        final LocalDateTime messageTimestamp = LocalDateTime.of(2021, Month.DECEMBER, 25, 23, 0, 0, 900);
        final String jsonMessage = "{ \"message\": \"A JSON message\", \"timestamp\": \"" + messageTimestamp.toString() + "\"}";

        NewRelicPipeline pipeline = new NewRelicPipeline(
                testPipeline,
                Create.of(message, jsonMessage),
                new NewRelicIO(getNewRelicConfig(url, null, 1, false)));

        pipeline.run().waitUntilFinish(Duration.millis(100));

        mockServerClient.verify(
                getRequestDefinition(),
                VerificationTimes.exactly(2));
    }

    private HttpRequest getRequestDefinition() {
        return HttpRequest.request(EXPECTED_PATH)
                .withMethod("POST")
                .withHeader(Header.header("Content-Type", "application/json"))
                .withHeader(Header.header("api-key", API_KEY));
    }

    // TODO Test that specifying null parameter options correctly use the default values (i.e. specifying null parallelism should result in parallelism=1)

    // TODO Test to check batching: sending 3 messages with a batching 2 results in 2 POST requests

    // TODO Test to check compression: check

    // TODO Test to check deadlettering: sending 2 messages with a batching of 1 and creating an expectation in the
    // MockServer that rejects the message with a 500 if the message content equals BLA. We ensure that the
    // rejected message ends up in the deadletter queue
}