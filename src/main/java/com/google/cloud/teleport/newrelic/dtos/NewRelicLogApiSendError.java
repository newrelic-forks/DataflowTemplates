package com.google.cloud.teleport.newrelic.dtos;

/**
 * A class for capturing errors when sending {@link NewRelicLogRecord}s to New Relic's Logs API end point.
 */
public class NewRelicLogApiSendError {

    /** Status code returned by the Logs API */
    private Integer statusCode;
    /** Status message returned by the Logs API, or message returned by any thrown exception */
    private String statusMessage;
    /** A JSON representation of the log record that was sent (but failed to be accepted) to the Logs API. */
    private String payload;

    public Integer getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
