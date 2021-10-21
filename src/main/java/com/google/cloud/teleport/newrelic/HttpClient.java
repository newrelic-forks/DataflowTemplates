package com.google.cloud.teleport.newrelic;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.StringUtils;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.HostnameVerifier;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HttpClient} is a utility class that helps write
 * {@link NewRelicLogRecord}s to a NewRelic Log API endpoint.
 */
public class HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);
    private static final int DEFAULT_MAX_CONNECTIONS = 1;
    private static final Set<Integer> RETRYABLE_STATUS_CODES = ImmutableSet.of(408, 429, 500, 502, 503, 504, 599);
    private static final String HTTPS_PROTOCOL_PREFIX = "https";
    private static final Gson GSON = new GsonBuilder().create();
    private static final Integer MAX_ELAPSED_MILLIS = ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS;
    private static final HttpBackOffUnsuccessfulResponseHandler RESPONSE_HANDLER;

    static {
        RESPONSE_HANDLER = new HttpBackOffUnsuccessfulResponseHandler(
                new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(MAX_ELAPSED_MILLIS).build()
        );
        RESPONSE_HANDLER.setBackOffRequired((HttpResponse response) -> RETRYABLE_STATUS_CODES.contains(response.getStatusCode()));
    }

    private final GenericUrl genericUrl;
    private final String apiKey;
    private final boolean useCompression;
    private ApacheHttpTransport transport;
    private HttpRequestFactory requestFactory;

    private HttpClient(final GenericUrl genericUrl,
                       final String apiKey,
                       final Boolean useCompression,
                       final ApacheHttpTransport transport,
                       final HttpRequestFactory requestFactory) {
        this.genericUrl = genericUrl;
        this.apiKey = apiKey;
        this.useCompression = useCompression;
        this.transport = transport;
        this.requestFactory = requestFactory;
    }

    /**
     * Initializes a {@link HttpClient} object.
     *
     * @return {@link HttpClient}
     */
    public static HttpClient init(
            final GenericUrl genericUrl,
            final String apiKey,
            final Boolean disableCertificateValidation,
            final Boolean useCompression) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        checkNotNull(apiKey, "API Key needs to be specified.");
        checkNotNull(genericUrl, "URL needs to be specified.");

        LOG.info("Certificate validation disabled: {}", disableCertificateValidation);
        LOG.info("Defaulting max backoff time to: {} milliseconds ", MAX_ELAPSED_MILLIS);

        CloseableHttpClient httpClient = getHttpClient(
                genericUrl.getScheme().equalsIgnoreCase(HTTPS_PROTOCOL_PREFIX),
                DEFAULT_MAX_CONNECTIONS,
                disableCertificateValidation);

        final ApacheHttpTransport transport = new ApacheHttpTransport(httpClient);

        return new HttpClient(
                genericUrl,
                apiKey,
                useCompression,
                transport,
                transport.createRequestFactory());
    }

    /**
     * Utility method to create a {@link CloseableHttpClient} to make http POSTs
     * against New Relic API.
     *
     * @param useSsl                       use SSL in the established connection
     * @param maxConnections               max number of parallel connections.
     * @param disableCertificateValidation should disable certificate validation.
     */
    private static CloseableHttpClient getHttpClient(final boolean useSsl, int maxConnections, boolean disableCertificateValidation)
            throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        HttpClientBuilder builder = ApacheHttpTransport.newDefaultHttpClientBuilder();

        if (useSsl) {
            LOG.info("SSL connection requested");

            HostnameVerifier hostnameVerifier = disableCertificateValidation ? NoopHostnameVerifier.INSTANCE
                    : new DefaultHostnameVerifier();

            SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
            if (disableCertificateValidation) {
                LOG.info("Certificate validation is disabled");
                sslContextBuilder.loadTrustMaterial((TrustStrategy) (chain, authType) -> true);
            }

            SSLConnectionSocketFactory connectionSocketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build(),
                    hostnameVerifier);
            builder.setSSLSocketFactory(connectionSocketFactory);
        }

        builder.setMaxConnTotal(maxConnections);
        builder.setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build());

        return builder.build();
    }

    /**
     * Executes a POST for the list of {@link NewRelicLogRecord} objects into New
     * Relic's log API.
     *
     * @param logRecords List of {@link NewRelicLogRecord}s
     * @return {@link HttpResponse} for the POST.
     */
    public HttpResponse send(final List<NewRelicLogRecord> logRecords) throws IOException {
        final HttpContent content = getContent(logRecords);

        final HttpRequest request = requestFactory.buildPostRequest(genericUrl, content);
        request.setUnsuccessfulResponseHandler(RESPONSE_HANDLER);
        setHeaders(request);

        return request.execute();
    }

    /**
     * Same as {@link HttpClient#send(List)} but with a single
     * {@link NewRelicLogRecord}.
     *
     * @param event {@link NewRelicLogRecord} object.
     */
    public HttpResponse send(NewRelicLogRecord event) throws IOException {
        return this.send(ImmutableList.of(event));
    }

    /**
     * Shutsdown connection manager and releases all resources.
     */
    public void close() throws IOException {
        if (transport != null) {
            LOG.info("Closing publisher transport.");
            transport.shutdown();
        }
    }

    /**
     * Utility method to set Authorization and other relevant http headers into the
     * {@link HttpRequest}.
     *
     * @param request {@link HttpRequest} object to add headers to.
     */
    private void setHeaders(HttpRequest request) {
        request.getHeaders().set("Api-Key", apiKey);
        if (useCompression) {
            request.getHeaders().set("Content-Encoding", "gzip");
        }
    }

    /**
     * Utility method to marshall a list of {@link NewRelicLogRecord}s into an
     * {@link HttpContent} object that can be used to create an {@link HttpRequest}.
     *
     * @param events List of {@link NewRelicLogRecord}s
     * @return {@link HttpContent} that can be used to create an
     * {@link HttpRequest}.
     */
    private HttpContent getContent(List<NewRelicLogRecord> events) {
        String payload = getStringPayload(events);
        // LOG.debug("Payload content: {}", payload);

        if (useCompression) {
            byte[] bytes = StringUtils.getBytesUtf8(payload);
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

            try (GZIPOutputStream gzipOut = new GZIPOutputStream(bytesOut)) {
                gzipOut.write(bytes);
                gzipOut.close();
                return new ByteArrayContent("application/gzip", bytesOut.toByteArray());
            } catch (IOException e) {
                LOG.warn("Couldn't gzip byte stream", e);
                return new ByteArrayContent("application/json", StringUtils.getBytesUtf8(payload));
            }
        } else {
            return new ByteArrayContent("application/json", StringUtils.getBytesUtf8(payload));
        }
    }

    /**
     * Utility method to get payload string from a list of {@link NewRelicLogRecord}s.
     */
    private String getStringPayload(List<NewRelicLogRecord> events) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        events.forEach(event -> sb.append(GSON.toJson(event)).append(','));
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }
}