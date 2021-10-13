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
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.HostnameVerifier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
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
 * {@link NewRelicEvent}s to a NewRelic Log API endpoint.
 */
public class HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private static final int DEFAULT_MAX_CONNECTIONS = 1;

    private static final Gson GSON = new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

    private static final String HTTPS_PROTOCOL_PREFIX = "https";

    private boolean useCompression;
    private ApacheHttpTransport transport;
    private HttpRequestFactory requestFactory;
    private GenericUrl genericUrl;
    private String apiKey;
    private Integer maxElapsedMillis = ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS;
    private Boolean disableCertificateValidation = false;


    public void setTransport(ApacheHttpTransport transport) {
        this.transport = transport;
    }

    public void setRequestFactory(HttpRequestFactory requestFactory) {
        this.requestFactory = requestFactory;
    }


    public void setGenericUrl(GenericUrl genericUrl) {
        this.genericUrl = genericUrl;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public void setDisableCertificateValidation(Boolean disableCertificateValidation) {
        this.disableCertificateValidation = disableCertificateValidation;
    }


    public void setUseCompression(Boolean useCompression) {
        this.useCompression = useCompression;
    }

    /**
     * Initializes a {@link HttpClient} object.
     *
     * @return {@link HttpClient}
     */
    public void init() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        checkNotNull(apiKey, "API Key needs to be specified.");
        checkNotNull(genericUrl, "URL needs to be specified.");

        LOG.info("Certificate validation disabled: {}", disableCertificateValidation);
        LOG.info("Defaulting max backoff time to: {} milliseconds ", maxElapsedMillis);

        CloseableHttpClient httpClient = getHttpClient(DEFAULT_MAX_CONNECTIONS, disableCertificateValidation);

        setTransport(new ApacheHttpTransport(httpClient));
        setRequestFactory(transport.createRequestFactory());
    }

    /**
     * Executes a POST for the list of {@link NewRelicEvent} objects into New
     * Relic's log API.
     *
     * @param events List of {@link NewRelicEvent}s
     * @return {@link HttpResponse} for the POST.
     */
    public HttpResponse execute(List<NewRelicEvent> events) throws IOException {

        HttpContent content = getContent(events);
        HttpRequest request = requestFactory.buildPostRequest(genericUrl, content);

        HttpBackOffUnsuccessfulResponseHandler responseHandler = new HttpBackOffUnsuccessfulResponseHandler(
                getConfiguredBackOff());

        responseHandler.setBackOffRequired(HttpBackOffUnsuccessfulResponseHandler.BackOffRequired.ON_SERVER_ERROR);

        request.setUnsuccessfulResponseHandler(responseHandler);
        setHeaders(request);

        return request.execute();
    }

    /**
     * Same as {@link HttpClient#execute(List)} but with a single
     * {@link NewRelicEvent}.
     *
     * @param event {@link NewRelicEvent} object.
     */
    public HttpResponse execute(NewRelicEvent event) throws IOException {
        return this.execute(ImmutableList.of(event));
    }

    /**
     * Return an {@link ExponentialBackOff} with the right settings.
     *
     * @return {@link ExponentialBackOff} object.
     */
    @VisibleForTesting
    protected ExponentialBackOff getConfiguredBackOff() {
        return new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(maxElapsedMillis).build();
    }

    /** Shutsdown connection manager and releases all resources. */
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
        if(useCompression) {
            request.getHeaders().set("Content-Encoding", "gzip");
        }
    }

    /**
     * Utility method to marshall a list of {@link NewRelicEvent}s into an
     * {@link HttpContent} object that can be used to create an {@link HttpRequest}.
     *
     * @param events List of {@link NewRelicEvent}s
     * @return {@link HttpContent} that can be used to create an
     *         {@link HttpRequest}.
     */
    private HttpContent getContent(List<NewRelicEvent> events) {
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
     * Utility method to get payload string from a list of {@link NewRelicEvent}s.
     */
    private String getStringPayload(List<NewRelicEvent> events) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        events.forEach(event -> sb.append(GSON.toJson(event)).append(','));
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }

    /**
     * Utility method to create a {@link CloseableHttpClient} to make http POSTs
     * against Splunk's API.
     *
     * @param maxConnections               max number of parallel connections.
     * @param disableCertificateValidation should disable certificate validation.
     */
    private CloseableHttpClient getHttpClient(int maxConnections, boolean disableCertificateValidation)
            throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        HttpClientBuilder builder = ApacheHttpTransport.newDefaultHttpClientBuilder();

        if (genericUrl.getScheme().equalsIgnoreCase(HTTPS_PROTOCOL_PREFIX)) {
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

}